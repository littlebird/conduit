(ns conduit.tools.transit-test
  (:require [cognitect.transit :as transit]
            [clojure.test :refer [testing is deftest]])
  (:import (java.io ByteArrayOutputStream
                    ByteArrayInputStream
                    InputStream)
           (com.cognitect.transit WriteHandler)))

(defn verbose-writer
  [d]
  (let [baos (ByteArrayOutputStream.)
        w (transit/writer baos :json)]
    (transit/write w d)
    ;; (println "encoded to" (String. (.toByteArray baos)))
    baos))

(deftest encoding-test
  (let [baos (verbose-writer "hello")
        r (transit/reader (ByteArrayInputStream. (.toByteArray baos)) :json)
        result (transit/read r)]
    (is (= result "hello"))))

(defn round-trip
  [d e m]
  (let [baos (ByteArrayOutputStream.)]
    (transit/write (transit/writer baos :json {:handlers m}) d)
    (is (= e (transit/read (transit/reader (ByteArrayInputStream. (.toByteArray baos)) :json))))))

(deftype Thing [name])

(deftest roundtrip-test
  (round-trip [:a :b :c]
              [:a :b :c]
              {})
  ;; MADNESS? THIS IS CLOJURE
  ;; (is (= (class (->Thing :a)) Thing))
  (round-trip (->Thing "hello")
              "hello"
              {Thing
               (reify WriteHandler
                 (tag [_ _] "'")
                 (rep [_ k] (.name k))
                 (stringRep [this k] (.rep this k))
                 (getVerboseHandler [_] nil))}))
