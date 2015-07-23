(ns conduit.tools
  #?@(:clj [(:require [taoensso.timbre :as timbre])
            (:import [com.cognitect.transit WriteHandler])]))

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
