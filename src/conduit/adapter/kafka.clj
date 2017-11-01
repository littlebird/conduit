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
         (assert (get-in component [:config :config :kafka :zk-connect])
                 "must specify zookeeper")
         (assert (get-in component [:config :config :kafka :kafka-connect])
                 "must specify kafka")
         (let [config (-> component :config :config :kafka)
               producer (kafka/make-producer (:kafka-connect config)
                                             (:producer-opts config))
               id (:id config (UUID/randomUUID))
               consumer-opts (assoc (:consumer-opts config)
                                    :host (:zk-connect config)
                                    :group (str group-prefix id))
               zk-consumer (kafka/make-consumer consumer-opts)
               topic-transmitter (fn topic-transmitter
                                   [topic]
                                   (make-routing-transmitter id
                                                             producer
                                                             topic
                                                             encoders))
               brokers #(zk/brokers {"zookeeper.connect" (:zk-connect config)})]
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
  [{:keys [owner encoders decoders socket-router group-prefix zk-host]
    :as config}]
  (map->KafkaPeer config))

(defn routing-decoder
  [decoders]
  (fn [msg]
    (let [inflated (kafka/decode-transit-baos msg decoders)
          [to routing sender message] inflated]
      [routing {:data message
                :sender sender
                :routing routing
                :to to}])))

(defn make-zk-routing-receiver
  [{:keys [my-id consumer group topic decoders request-chan]}]
  (let [decode (routing-decoder decoders)
        stream (consumer/create-message-stream consumer topic)
        it (.iterator stream)
        result (>/chan)]
    (future
      (try
        (loop []
          (let [wait (delay (if request-chan
                              (>/<!! request-chan)
                              true))
                msg (delay (.message (.next it)))
                payload (delay (decode @msg))
                to (delay (:to (second @payload)))
                send-result (delay (when (or (not @to)
                                             (= @to my-id))
                                     (>/>!! result @payload))
                                   true)]
            (and @wait
                 @msg
                 @payload
                 @send-result
                 (recur))))
        (catch Exception e
          (println "Error in kafka conduit zk-receiver" (pr-str e)))))
    result))

(defn new-kafka-conduit
  [{:keys [request-chan group topic send-topic verbose unhandled impl]}]
  (let [{:keys [id topic-transmitter producer zk-consumer brokers]} impl
        {:keys [encoders decoders]} impl
        my-id (or id (UUID/randomUUID))
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
