(ns conduit.adapter.kafka
  (:require [clj-kafka.zk :as zk]
            [conduit.protocol :as conduit]
            [conduit.tools :as tools]
            [cognitect.transit :as transit]
            [clj-kafka.producer :as produce]
            [clj-kafka.consumer.zk :as zk-consume]
            [clojure.core.async :as >]
            [noisesmith.component :as component]
            [conduit.tools.component-util :as util])
  (:import (java.util UUID
                      Date)
           (java.io ByteArrayInputStream
                    ByteArrayOutputStream)))


;;; CREATE TOPIC
;; bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test
(defn make-topic
  [topic])

;;; FIND TOPICS
;; bin/kafka-topics.sh --list --zookeeper localhost:2181
(defn list-topics
  [])

(defn encoded-transmitter
  [producer encoders]
  (fn [topic data]
    (let [baos (ByteArrayOutputStream. 512)
          writer (transit/writer baos :json encoders)
          _ (transit/write writer data)
          packed (produce/message topic (.toByteArray baos))]
      (produce/send-message producer packed))))

(defn make-transmitter
  [my-id producer topic encoders]
  (let [encoded-transmit (encoded-transmitter producer encoders)]
    (fn kafka-transmitter
      [to route message]
      (let [data [to route my-id message]]
        (encoded-transmit topic data)))))

(defn make-producer
  [broker opts]
  (produce/producer
   (merge
    {"metadata.broker.list" broker ; string host:port
     "serializer.class" "kafka.serializer.DefaultEncoder"
     "partitioner.class" "kafka.producer.DefaultPartitioner"})))

(defrecord KafkaPeer [encoders decoders socket-router group-prefix owner]
  component/Lifecycle
  (start [component]
    (util/start
     component
     :kafka-peer
     owner
     (fn []
       (assert (-> component :config :config :kafka :zk-host) "must specify host")
       (let [config (-> component :config :config :kafka)
             config (merge
                     {:zk-port 2181
                      :kafka-port 9092}
                     config)
             producer (make-producer (str (:zk-host config) \:
                                          (:kafka-port config))
                                     (:producer-opts config))
             id (:id config (UUID/randomUUID))
             zk-consumer (zk-consume/consumer
                          (merge
                           {"zookeeper.connect" (str (:zk-host config) \:
                                                     (:zk-port config))
                            "group.id" (str group-prefix id)
                            "auto.offset.reset" "largest"
                            "auto.commit.interval.ms" "200"
                            "auto.commit.enable" "true"}
                           (:consumer-opts config)))
             topic-transmitter (fn topic-transmitter
                                 [topic]
                                 (make-transmitter id producer topic encoders))
             brokers #(zk/brokers
                       {"zookeeper.connect"
                        (str (:zk-host config) \: (:zk-port config))})]
         (assoc component
                :kafka-peer :started
                :topic-transmitter topic-transmitter
                :id id
                :socket-router socket-router
                :encoders encoders
                :decoders decoders
                :brokers brokers
                :producer producer
                :zk-consumer zk-consumer)))))
  (stop [component]
    (util/stop
     component
     :kafka-peer
     owner
     (fn []
       (dissoc component
               :kafka-peer
               :encoders
               :decoders
               :producer
               :zk-consumer
               :brokers)))))

(defn new-kafka-peer
  [{:keys [owner encoders decoders socket-router group-prefix zk-host] :as config}]
  (map->KafkaPeer config))

(defrecord KafkaConduit [transmitter receiver
                         verbose unhandled
                         id]
  conduit/Conduit
  (identifier [this]
    "kafka conduit channel")
  (verbose? [this]
    (some-> verbose deref))
  (receiver [this]
    receiver)
  (parse [this [routing contents]]
    ;; get the routing, contents, response function from the message / instance
    {:routing routing
     :contents contents
     :transmit transmitter})
  (unhandled [this message provided]
    (unhandled message)))

(defn decode-transit-baos
  [baos decoders]
  (let [bytes-in (ByteArrayInputStream. baos)
        reader (transit/reader bytes-in :json decoders)]
     (transit/read reader)))

(defn decoder
  [decoders]
  (fn [msg]
    (let [[to routing sender message :as inflated] (decode-transit-baos msg decoders)]
      [routing {:data message
                :sender sender
                :routing routing
                :to to}])))

(defn make-zk-receiver
  [{:keys [my-id consumer group topic decoders request-chan]}]
  (let [decode (decoder decoders)
        stream (zk-consume/create-message-stream consumer topic)
        it (.iterator stream)
        get-next-message #(.message (.next it))
        result (>/chan)]
    (future
      (loop [msg (get-next-message)]
        (try
          (let [payload (decode msg)
                to (:to (second payload))]
            (when (and payload
                       (or (not to)
                                   (= to my-id)))
              (>/>!! result payload))
            (when request-chan
              ;; if supplied, request-chan allows "pull" of messages - you can let
              ;; other peers in your group take a message by not putting messages onto this
              ;; channel
              (println "Kafka Conduit in group" group "waiting before grabbing a job from topic" topic "as requested.")
              (>/<!! request-chan)
              (println "Kafka Conduit in group" group "grabbing a job from topic" topic ".")))
          (catch Exception e
            (println "Error in kafka conduit zk-receiver" (pr-str e))))
        (recur (get-next-message))))
    result))

(defn new-kafka-conduit
  [{{:keys [id topic-transmitter producer zk-consumer brokers encoders decoders] :as impl} :impl
    request-chan :request-chan
    group :group
    topic :topic
    send-topic :send-topic
    verbose :verbose
    unhandled :unhandled}]
  (let [my-id (or id (UUID/randomUUID))
        zk-receiver (make-zk-receiver {:my-id my-id
                                       :consumer zk-consumer
                                       :group group
                                       :topic topic
                                       :decoders decoders
                                       :request-chan request-chan})
        send-topic (or send-topic topic)]
    (map->KafkaConduit {:transmitter (topic-transmitter send-topic)
                        :receiver zk-receiver
                        :verbose verbose
                        :unhandled unhandled
                        :id my-id})))
