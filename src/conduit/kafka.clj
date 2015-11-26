(ns conduit.kafka
  (:require [clj-kafka.consumer.zk :as zk-consume]
            [cognitect.transit :as transit]
            [clj-kafka.producer :as produce])
  (:import (java.io ByteArrayInputStream
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

(defn make-producer
  [broker opts]
  (produce/producer
   (merge
    {"metadata.broker.list" broker ; string host:port
     "serializer.class" "kafka.serializer.DefaultEncoder"
     "partitioner.class" "kafka.producer.DefaultPartitioner"})))

(defn decode-transit-baos
  [baos decoders]
  (let [bytes-in (ByteArrayInputStream. baos)
        reader (transit/reader bytes-in :json decoders)]
     (transit/read reader)))

(defn make-consumer
  [opts]
  (zk-consume/consumer
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
  (let [stream (zk-consume/create-message-stream consumer topic)
        it (.iterator stream)
        get-next-message #(.message (.next it))]
    get-next-message))


