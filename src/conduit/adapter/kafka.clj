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

(defrecord KafkaConduit [transmitter receiver-fn
                         verbose unhandled
                         id]
  conduit/Conduit
  (identifier [this]
    "kafka conduit channel")
  (verbose? [this]
    (some-> verbose deref))
  (receiver [this]
    (receiver-fn))
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

(defn construct-kafka-client
  [{:keys [consumer topic decoders]}]
  (let [decode (routing-decoder decoders)
        stream (consumer/create-message-stream consumer topic)
        message-iterator (.iterator stream)]
    {:decode decode
     :stream stream
     :message-iterator message-iterator}))

(defn make-zk-routing-receiver
  "returns a channel which will receive each message that the client should act
   on, if the request-chan arg is supplied, the receiver will wait until it
   gets a ready message (the content of which is ignored) before getting each
   item from the network (this lets us pull instead of them pushing)"
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
                          (println ::zk-routing-receiver (pr-str e))
                          nil))]
         (let [for-me (or (not to)
                          (= to my-id))
               done? (when for-me
                       (not (>/>!! result next-input)))]
           (when-not done?
             (recur))))))
    (constantly  result)))

(defn make-async-routing-receiver
  "like make-zk-routing-receiver but instead of just blocking until it gets
   a ready message, it expects a message containing a channel onto which to
   put its result on each loop"
  [opts]
  {:pre [(:request-chan opts)]}
  (let [{:keys [my-id consumer group topic decoders request-chan get-client]} opts
        construct-client (or get-client construct-kafka-client)
        {:keys [decode message-iterator]} (construct-client opts)]
    (letfn [(get-message-from-stream [result-chan]
              (when-let [message (.message (.next message-iterator))]
                {:message message
                 :result-chan result-chan}))
            (get-message-payload [{:keys [message] :as context}]
                                 (assoc context :payload (decode message)))
            (check-ignore [{:keys [payload] :as context}]
                          (let [[_  {:keys [to]}] payload]
                            (assoc context :ignore? (and to
                                                         (not= my-id to)))))
            (maybe-send-result [{:keys [payload result-chan ignore?]}]
                               (if ignore?
                                 result-chan
                                 (and (>/>!! result-chan payload)
                                      false)))]
      (future-call
       (fn async-routing-receiver
         ([] (async-routing-receiver false))
         ([send-chan]
           ;; if target-chan is false, we get a new one, otherwise reuse it
          (let [target-chan (or send-chan
                                (>/<!! request-chan))
                maybe-sent (try (some-> target-chan
                                        (get-message-from-stream)
                                        (get-message-payload)
                                        (check-ignore)
                                        (maybe-send-result))
                                (catch Exception e
                                  (println ::async-routing-receiver (pr-str e))
                                  nil))]
             ;; if nothing in maybe-sent returned nil,
             ;; recur with the next channel to use
            (some-> maybe-sent
                    (recur)))))))
    (fn []
      (let [res-chan (>/chan)]
        (>/>!! request-chan res-chan)
        res-chan))))

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
                       :request-chan request-chan}
        zk-receiver-fn (if request-chan
                         (make-async-routing-receiver receiver-args)
                         (make-zk-routing-receiver receiver-args))
        send-topic (or send-topic topic)]
    (map->KafkaConduit {:transmitter (topic-transmitter send-topic)
                        :receiver-fn zk-receiver-fn
                        :verbose verbose
                        :unhandled unhandled
                        :id my-id})))
