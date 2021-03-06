(ns conduit.adapter.sente
  (:require [conduit.protocol :as conduit]
            [conduit.tools :as tools]))

(defrecord SenteConduit [impl verbose unhandled parse-callback transmit-wrapper]
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
                        ((or transmit-wrapper partial) (:send-fn impl) uid)
                        #(tools/error-msg (str "tried to send" %& "with no UID")))
                      :cljs
                      ((or transmit-wrapper identity) (:send-fn impl)))
          result {:routing routing
                  :contents contents
                  :transmit transmit
                  :uid (:uid message)}]
      (when parse-callback
        (parse-callback result))
      result))
  (unhandled [this message provided]
    (unhandled message provided)))

(defn new-sente-conduit
  [opts]
  (map->SenteConduit opts))
