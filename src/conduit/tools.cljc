(ns conduit.tools
  #?@(:clj
      [(:require [taoensso.timbre :as timbre]
                 [clojure.data.codec.base64 :as b64]
                 [cognitect.transit :as transit])
       (:import (com.cognitect.transit WriteHandler)
                (java.io ByteArrayOutputStream
                         ByteArrayInputStream))]
      :cljs
      [(:require [goog.crypt.base64 :as b64]
                 [cljs.reader :as reader]
                 [cognitect.transit :as transit])]))

(defn error-msg
  [str]
  #?(:clj
     (timbre/error str)
     :cljs
     (.log js/console str)))

(defn debug-msg
  [str]
  #?(:clj
     (timbre/debug str)
     :cljs
     (.log js/console str)))

(def writer-proxy
  #?(:clj
     (reify WriteHandler
       (tag [_ _] "'")
       (rep [_ o] (str o))
       (stringRep [_ o] (str o))
       (getVerboseHandler [_] nil))))

(defn transit-pack
  [data encoders]
  #?(:clj
     (let [baos (ByteArrayOutputStream. 512)
           writer (transit/writer baos :json encoders)
           _ (transit/write writer data)]
       baos)
     :cljs
     (transit/write (transit/writer :json encoders) data)))

(defn transit-unpack-bytes
  [bytes decoders]
  #?(:clj
     (let [bytes-in (ByteArrayInputStream. bytes)
           reader (transit/reader bytes-in :json decoders)]
       (transit/read reader))
     :cljs
     (transit/read (transit/reader :json decoders) bytes)))

(defn transit-unpack
  [msg decoders]
  #?(:clj
     (transit-unpack-bytes (.getBytes msg) decoders)
     :cljs
     (transit-unpack-bytes msg decoders)))

(def b64-encode-bytes-raw
  #?(:clj
     b64/encode
     :cljs
     b64/encodeString))

(def b64-encode-bytes
  #?(:clj
     #(b64/encode (.getBytes %))
     :cljs
     b64/encodeString))

(def b64-encode
  #?(:clj
     #(String. (b64-encode-bytes %))
     :cljs
     b64/encodeString))

(def b64-decode-bytes-raw
  #?(:clj
     b64/decode
     :cljs
     b64/decodeString
     #_
     #(js/unescape
       (js/encodeURIComponent
        (b64/decodeString %)))))

(def b64-decode-bytes
  #?(:clj
     #(b64/decode (.getBytes %))
     :cljs
     b64-decode-bytes-raw))

(def b64-decode
  #?(:clj
     #(String. (b64-decode-bytes %))
     :cljs
     b64-decode-bytes-raw))

;; These exist because we've found errors with taking a string generated
;; by transit, splitting it, putting the parts in data structures, encoding
;; those with transit, and finally reconstructing on another host. Oddly
;; it was passing the tests, but when it crosses hosts it breaks unicode
;; strings. The <strike>base64</strike> map int step prevents this error.
(defn packup-for-split
  [datum encoders]
  (-> datum
      (transit-pack encoders)
      #?(:clj (.toString))
      (->>
       ;; thus increasing message size by at least 3x, fuck
       (map int))
      (pr-str)))

(defn unpack-decode-joined
  [encoded decoders]
  (-> encoded
      #?(:clj
         (read-string)
         :cljs
         (reader/read-string))
      (->>
       (map char)
       (apply str))
      (transit-unpack decoders)
      #?(:cljs
         (doto (prn "(in unpack-decode-joined)")))))

;; (re-find #"\ufeff" stupid-broken-edn)
