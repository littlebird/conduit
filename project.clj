(defproject littlebird-aviary/conduit "0.0.10"
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.clojure/core.async "0.1.346.0-17112a-alpha"]
                 [com.taoensso/encore "1.38.0"]
                 [com.taoensso/timbre "3.3.1"
                  :exclusions [com.taoensso/encore]]
                 [noisesmith/component "0.2.5"]
                 [prismatic/schema "0.4.3"]
                 [clj-kafka "0.3.1"]
                 [com.cognitect/transit-clj "0.8.275"]])
