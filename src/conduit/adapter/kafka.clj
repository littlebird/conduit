(ns conduit.adapter.kafka
  (:require [conduit.protocol :as conduit]
            [conduit.tools :as tools]
            [cognitect.transit :as transit]
            [clj-kafka.producer :as produce]
            [clj-kafka.consumer.zk :as zk-consume]
            [clojure.core.async :as >])
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

(defn decoder
  [decoders]
  (fn [msg]
    (let [bytes-in (ByteArrayInputStream. msg)
          reader (transit/reader bytes-in :json decoders)
          [to routing sender message :as inflated] (transit/read reader)]
      [routing (assoc (if (map? message)
                        message
                        {:data message})
                      :sender sender
                      :raw inflated
                      :msg msg
                      :to to)])))

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

(defn make-transmitter
  [my-id producer topic encoders]
  (fn [to route message]
    ;; easy optimization -- pooling or other re-use of encoders
    (let [data [to route my-id message]
          baos (ByteArrayOutputStream. 512)
          writer (transit/writer baos :json encoders)
          _ (transit/write writer data)
          packed (produce/message topic (.toByteArray baos))]
      (produce/send-message producer packed))))

(defn new-kafka-conduit
  [{{:keys [id producer zk-consumer brokers encoders decoders] :as impl} :impl
    group :group
    topic :topic
    verbose :verbose
    unhandled :unhandled}]
  (let [my-id (or id (UUID/randomUUID))
        transmitter (make-transmitter my-id producer topic encoders)
        zk-receiver (make-zk-receiver {:my-id my-id
                                       :consumer zk-consumer
                                       :group group
                                       :topic topic
                                       :decoders decoders})]
    (map->KafkaConduit {:transmitter transmitter
                        :receiver zk-receiver
                        :verbose verbose
                        :unhandled unhandled
                        :id my-id})))
