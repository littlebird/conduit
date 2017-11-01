(defproject littlebird-aviary/conduit "0.2.46"
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/core.async "0.3.443"]
                 [com.taoensso/encore "2.92.0"]
                 [com.taoensso/timbre "4.10.0"
                  :exclusions [com.taoensso/encore]]
                 [noisesmith/component "0.2.5"]
                 [org.clojure/data.codec "0.1.0"]
                 [prismatic/schema "1.1.7"]
                 [clj-kafka "0.3.4"]
                 [com.taoensso/sente "1.4.1"] ; pinned
                 [com.cognitect/transit-clj "0.8.300"]])
