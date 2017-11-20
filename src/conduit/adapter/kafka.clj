(ns conduit.adapter.kafka
  (:require [clj-kafka.zk :as zk]
            [clj-kafka.consumer.zk :as consumer]
            [taoensso.timbre :as timbre]
            [conduit.kafka :as kafka]
            [conduit.protocol :as conduit]
            [conduit.tools :as tools]
            [clojure.core.async :as >]
            [conduit.adapter.async :as async]
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

(defrecord KafkaConduit [transmitter receiver-fn
                         verbose unhandled
                         id]
  conduit/Conduit
  (identifier [this]
    "kafka conduit channel")
  (verbose? [this]
    (some-> verbose deref))
  (receiver [this]
    (let [received (receiver-fn)]
      received))
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
         (catch Exception e (timbre/error ::KafkaPeer$start
                                          "error starting kafka peer" e)
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

(defn construct-kafka-client
  [{:keys [consumer topic decoders]}]
  (let [decode (routing-decoder decoders)
        stream (consumer/create-message-stream consumer topic)
        message-iterator (.iterator stream)]
    {:decode decode
     :stream stream
     :message-iterator message-iterator}))

(defn make-zk-routing-receiver
  "returns a function returning a channel which will receive a message that the
   client should act on"
  [opts]
  (let [{:keys [my-id consumer group topic decoders get-client]} opts
        construct-client (or get-client construct-kafka-client)
        {:keys [decode message-iterator]} (construct-client opts)
        result (>/chan)]
    (future-call
     (fn zk-routing-receiver []
       (when-some [[_ {:keys [to]} :as next-input]
                   (try (some-> (.next message-iterator)
                                (.message)
                                (decode))
                        (catch Exception e
                          (timbre/error ::zk-routing-receiver (pr-str e))
                          nil))]
         (let [for-me (or (not to)
                          (= to my-id))
               done? (when for-me
                       (not (>/>!! result next-input)))]
           (when-not done?
             (recur))))))
    (constantly  result)))

(defn get-message-from-stream
  [message-iterator work-chan]
  (when-let [message (.message (.next message-iterator))]
    {:message message
     :work-chan work-chan}))

(defn make-async-routing-receiver
  "like make-zk-routing-receiver but instead of just blocking until it gets
   a ready message, it expects a message containing a channel onto which to
   put its result on each loop"
  [{:keys [capacity-chan] :as opts}]
  {:pre [capacity-chan]}
  (let [{:keys [get-client]} opts
        construct-client (or get-client construct-kafka-client)
        {:keys [message-iterator] :as client} (construct-client opts)
        facilities (assoc client
                          :get-message-from-stream get-message-from-stream
                          :message-iterator message-iterator)]
    (async/make-routing-receiver opts facilities)))

(defn new-kafka-conduit
  [{:keys [request-chan group topic send-topic verbose unhandled impl]}]
  (let [{:keys [id topic-transmitter producer zk-consumer brokers]} impl
        {:keys [encoders decoders]} impl
        my-id (or id (UUID/randomUUID))
        receiver-args {:my-id my-id
                       :consumer zk-consumer
                       :group group
                       :topic topic
                       :decoders decoders
                       :capacity-chan request-chan}
        zk-receiver-fn (if request-chan
                         (make-async-routing-receiver receiver-args)
                         (make-zk-routing-receiver receiver-args))
        send-topic (or send-topic topic)]
    (map->KafkaConduit {:transmitter (topic-transmitter send-topic)
                        :receiver-fn zk-receiver-fn
                        :verbose verbose
                        :unhandled unhandled
                        :id my-id})))
