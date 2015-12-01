(ns conduit.adapter.kafka
  (:require [clj-kafka.zk :as zk]
            [clj-kafka.consumer.zk :as consumer]
            [conduit.kafka :as kafka]
            [conduit.protocol :as conduit]
            [conduit.tools :as tools]
            [clojure.core.async :as >]
            [noisesmith.component :as component]
            [conduit.tools.component-util :as util])
  (:import (java.util UUID)))

(defn make-routing-transmitter
  [my-id producer topic encoders]
  (let [encoded-transmit (kafka/encoded-transmitter producer encoders)]
    (fn kafka-transmitter
      [to route message]
      (let [data [to route my-id message]]
        (encoded-transmit topic data)))))

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

(defrecord KafkaPeer [encoders decoders socket-router group-prefix owner]
  component/Lifecycle
  (start [component]
    (util/start
     component
     :kafka-peer
     owner
     (fn []
       (try
         (assert (-> component :config :config :kafka :zk-host) "must specify host")
         (let [config (-> component :config :config :kafka)
               config (merge
                       {:zk-port 2181
                        :kafka-port 9092}
                       config)
               producer (kafka/make-producer (str (:zk-host config) \:
                                                  (:kafka-port config))
                                             (:producer-opts config))
               id (:id config (UUID/randomUUID))
               zk-consumer (kafka/make-consumer (assoc (:consumer-opts config)
                                                       :host (str (:zk-host config) \: (:zk-port config))
                                                       :group (str group-prefix id)))
               topic-transmitter (fn topic-transmitter
                                   [topic]
                                   (make-routing-transmitter id producer topic encoders))
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
                  :zk-consumer zk-consumer))
         (catch Exception e (println "error starting kafka peer" e)
                (throw e))))))
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

(defn routing-decoder
  [decoders]
  (fn [msg]
    (let [[to routing sender message :as inflated] (kafka/decode-transit-baos msg decoders)]
      [routing {:data message
                :sender sender
                :routing routing
                :to to}])))

(defn make-zk-routing-receiver
  [{:keys [my-id consumer group topic decoders request-chan]}]
  (let [decode (routing-decoder decoders)
        stream (consumer/create-message-stream consumer topic)
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
              (println "Kafka Conduit in group" group
                       "waiting before grabbing a job from topic" topic
                       "as requested.")
              (>/<!! request-chan)
              (println "Kafka Conduit in group" group
                       "grabbing a job from topic" (str topic "."))))
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
        zk-receiver (make-zk-routing-receiver {:my-id my-id
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
