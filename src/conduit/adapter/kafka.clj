(ns conduit.adapter.kafka
  (:require [conduit.protocol :as conduit]
            [conduit.tools :as tools]
            [cognitect.transit :as transit]
            [clj-kafka.producer :as produce]
            [clj-kafka.consumer.simple :as consume]
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
                         verbose unhandled]
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

(defn new-kafka-conduit
  [{:keys [{:keys [producer consumer brokers encoders decoders] :as impl}
           group
           topic
           verbose
           unhandled
           id]}]
  (let [my-id (or id (UUID/randomUUID))
        from (consume/latest-topic-offset consumer topic 0)
        transmitter
        (fn [route message]
          ;; easy optimization -- pooling or other re-use of encoders
          (let [data [route my-id message]
                baos (ByteArrayOutputStream. 512)
                writer (transit/writer baos :json encoders)
                _ (transit/write writer data)
                packed (produce/message baos)]
            (produce/send-message producer topic packed)))
        receiver (let [results (>/chan)]
                   (>/go-loop [in nil
                               since (.getTime (Date.))
                               timeout 100]
                     (doseq [msg in]
                       (let [bytes-in (ByteArrayInputStream. (:value msg))
                             reader (transit/reader bytes-in decoders)
                             [to routing message] (transit/read reader)]
                         (when (= (or to my-id)
                                  my-id)
                           (>/>! results [routing (assoc message :raw msg)]))))
                     (>/timeout timeout)
                     (recur (consume/messages consumer group
                                              topic 0 from 1000)
                            (if (not-empty in) (.getTime (Date.)) since)
                            ;; TODO: fine tune
                            timeout))
                   results)]
    (map->KafkaConduit {:transmitter transmitter
                        :receiver receiver
                        :verbose verbose
                        :unhandled unhandled})))
