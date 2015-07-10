(ns conduit.tools
  #?(:clj (:require [taoensso.timbre :as timbre])))

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
