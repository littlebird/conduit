(ns conduit.protocol
  #?(:clj (:import (java.util UUID)
                   (clojure.lang Keyword
                                 ILookup))))

(defprotocol Conduit
  (identifier [this])
  (verbose? [this])
  (receiver [this])
  (parse [this message])
  (unhandled [this message provided]))

(defprotocol IStation
  (where [this])
  (which [this])
  (validate-station [this]))

(defrecord Station
#?@(:clj
     [[^Keyword where
       ^UUID which]
      IStation
      (where [this] where)
      (which [this] which)
      (validate-station [this]
                        (assert (keyword? where))
                        (assert (instance? UUID which))
                        this)]
     :cljs
     [[]]))

(defprotocol IConduitMessage
#?@(:clj
     [(origin [this])
      (data [this])
      (validate-message [this])
      (propagate [this ^IStation from ^ILookup data])]))

(defrecord ConduitMessage
#?@(:clj
     [[^ILookup data
       itenerary ; keyword sequence
       passport ; station sequence
       ^ILookup user-data]
      IConduitMessage
      (origin [this]
              (which (first passport)))
      (data [this]
            data)
      (validate-message [this]
                        (assert  (map? data))
                        (assert (sequential? itenerary))
                        (assert (every? keyword? itenerary))
                        (assert (sequential? passport))
                        (assert (every? validate-station passport))
                        (assert (map? user-data))
                        this)
      (propagate [this from data]
                 (validate-message this)
                 (validate-message
                  (ConduitMessage.
                   data
                   itenerary
                   (conj (vec passport) from)
                   user-data)))]
     :cljs
     [[]]))
