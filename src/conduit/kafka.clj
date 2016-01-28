(ns conduit.kafka
  (:require [cognitect.transit :as transit]
            [clj-kafka.consumer.zk :as consumer]
            [clj-kafka.new.producer :as producer])
  (:import (java.io ByteArrayInputStream
                    ByteArrayOutputStream)
           org.slf4j.LoggerFactory
           (ch.qos.logback.classic Logger Level)))

(defn stfu-up
  []
  (.setLevel (LoggerFactory/getLogger Logger/ROOT_LOGGER_NAME)
             Level/WARN))

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
          packed (producer/record topic (.toByteArray baos))]
      (producer/send producer packed))))

(defn make-producer
  [broker opts]
  (producer/producer
   (merge
    {"bootstrap.servers" broker} ; string host:port
    opts)
   (producer/byte-array-serializer)
   (producer/byte-array-serializer)))

(defn decode-transit-baos
  [baos decoders]
  (let [bytes-in (ByteArrayInputStream. baos)
        reader (transit/reader bytes-in :json decoders)]
     (transit/read reader)))

(defn make-consumer
  [opts]
  (consumer/consumer
   (merge
    {"zookeeper.connect" (:host opts)
     "group.id" (:group opts)
     "auto.offset.reset" "largest"
     "auto.commit.interval.ms" "200"
     "auto.commit.enable" "true"}
    (reduce-kv (fn [m k v]
                 (when (string? k) (assoc m k v)))
               {}
               opts))))

(defn zk-topic-source
  [consumer topic]
  (let [stream (consumer/create-message-stream consumer topic)
        it (.iterator stream)
        get-next-message #(.message (.next it))]
    get-next-message))
