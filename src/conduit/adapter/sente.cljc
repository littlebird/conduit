(ns conduit.adapter.sente
  (:require [conduit.protocol :as conduit]
            [conduit.tools :as tools]))

(defrecord SenteConduit [impl verbose unhandled]
  conduit/Conduit
  (identifier [this]
    "sente conduit channel")
  (verbose? [this]
    (some-> verbose deref))
  (receiver [this]
    (:ch-recv impl))
  (parse [this message]
    (let [[routing contents] (:event message)
          [routing contents] (if (= routing :chsk/recv)
                               contents
                               [routing contents])
          transmit #?(:clj
                      (if-let [uid (:uid contents)]
                        (partial (:send-fn impl) uid)
                        #(tools/error-msg "tried to send" %& "with no UID"))
                      :cljs
                      (:send-fn impl))
          result {:routing routing
                  :contents contents
                  :transmit transmit}]
      #?(:clj
         result
         :cljs
         (assoc result
                :uid (:uid message)))))
  (unhandled [this message provided]
    (unhandled message provided)))
