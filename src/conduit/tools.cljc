(ns conduit.tools
  #?@(:clj
      [(:require [taoensso.timbre :as timbre]
                 [cognitect.transit :as transit])
       (:import (com.cognitect.transit WriteHandler)
                (java.io ByteArrayOutputStream
                         ByteArrayInputStream))]
      :cljs
      [(:require [cognitect.transit :as transit])]))

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

(defn transit-unpack
  [msg decoders]
  #?(:clj
     (let [bytes-in (ByteArrayInputStream. msg)
           reader (transit/reader bytes-in :json decoders)]
       (transit/read reader))
     :cljs
     (transit/read (transit/reader :json decoders) msg)))
