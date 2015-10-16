(ns conduit.adapter.kafka
  (:require [clj-kafka.zk :as zk]
            [conduit.protocol :as conduit]
            [conduit.tools :as tools]
            [clj-kafka.producer :as produce]
            [clj-kafka.consumer.zk :as zk-consume]
            [clojure.core.async :as >]
            [noisesmith.component :as component]
            [conduit.partial-messages :as partial]
            [conduit.tools.component-util :as util])
  (:import (java.util UUID
                      Date)))


;;; CREATE TOPIC
;; bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test
(defn make-topic
  [topic])

;;; FIND TOPICS
;; bin/kafka-topics.sh --list --zookeeper localhost:2181
(defn list-topics
  [])

(defn make-transmitter
  [my-id producer topic encoders message-split-threshold]
  (partial/wrap-transmit-separate
   (fn kafka-transmitter
     [to route message]
     ;; easy optimization -- pooling or other re-use of baos
     (let [message (when (map? message) (assoc message :conduit/provinence :conduit/kafka))
           data [to route my-id message]
           baos (tools/transit-pack data encoders)
           packed (produce/message topic (.toByteArray baos))]
       (produce/send-message producer packed)))
   message-split-threshold
   encoders))

(defrecord KafkaPeer [encoders decoders socket-router group-prefix owner
                      message-split-threshold]
  component/Lifecycle
  (start [component]
    (util/start
     component
     :kafka-peer
     owner
     (fn []
       (assert (get-in component [:config :config :kafka :zk-host]) "must specify host")
       (let [config (get-in component [:config :config :kafka])
             config (merge
                     {:zk-port 2181
                      :kafka-port 9092}
                     config)
             producer (produce/producer
                       (merge
                        {"metadata.broker.list" (str (:zk-host config) \: (:kafka-port config))
                         "serializer.class" "kafka.serializer.DefaultEncoder"
                         "partitioner.class" "kafka.producer.DefaultPartitioner"}
                        (:producer-opts config)))
             id (:id config (UUID/randomUUID))
             zk-consumer (zk-consume/consumer
                          (merge
                           {"zookeeper.connect" (str (:zk-host config) \: (:zk-port config))
                            "group.id" (str group-prefix id)
                            "auto.offset.reset" "largest"
                            "auto.commit.interval.ms" "200"
                            "auto.commit.enable" "true"}
                           (:consumer-opts config)))
             topic-transmitter (fn topic-transmitter
                                 [topic]
                                 (make-transmitter id producer topic encoders
                                                   message-split-threshold))
             brokers #(zk/brokers {"zookeeper.connect"
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
  [{:keys [owner encoders decoders socket-router group-prefix zk-host
           message-split-threshold] :as config}]
  (map->KafkaPeer config))

(defrecord KafkaConduit [transmitter receiver
                         verbose unhandled
                         id partial-parser]
  conduit/Conduit
  (identifier [this]
    "kafka conduit channel")
  (verbose? [this]
    (some-> verbose deref))
  (receiver [this]
    receiver)
  (parse [this [routing contents]]
    (let [combined (partial-parser (:data contents))]
      (if (= combined :partial/consumed)
        :partial/consumed
        {:routing routing
         :contents (assoc contents :data combined)
         :transmit (fn [& args]
                     (when @verbose (println "kafka conduit transmitting" (pr-str args)))
                     (apply transmitter args))})))
  (unhandled [this message provided]
    (unhandled message)))

(defn decoder
  [decoders]
  (fn [msg]
    (let [[to routing sender message :as inflated]
          (tools/transit-unpack-bytes msg decoders)]
      [routing {:data message
                :sender sender
                :routing routing
                :to to}])))

(defn make-zk-receiver
  [{:keys [my-id consumer topic decoders]}]
  (let [decode (decoder decoders)
        stream (zk-consume/create-message-stream consumer topic)
        it (.iterator stream)
        result (>/chan)]
    (>/go-loop [msg (.message (.next it))]
      (let [payload (decode msg)
            to (:to (second payload))]
        (when (or (not to)
                  (= to my-id))
          (>/>! result payload))
        (recur (.message (.next it)))))
    result))

(defn new-kafka-conduit
  [{{:keys [id topic-transmitter producer zk-consumer brokers encoders decoders] :as impl} :impl
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
                                       :decoders decoders})
        send-topic (or send-topic topic)
        partial-parser (partial/wrap-parser-result decoders)]
    (map->KafkaConduit {:transmitter (topic-transmitter send-topic)
                        :receiver zk-receiver
                        :verbose verbose
                        :unhandled unhandled
                        :id my-id
                        :partial-parser partial-parser})))
