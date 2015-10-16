(ns conduit.adapter.sente
  (:require [conduit.protocol :as conduit]
            [conduit.partial-messages :as partial]
            [conduit.tools :as tools]))
(defn maybe-verbose
  [f]
  (fn perhaps-verbose-transmission
    [& args]
    (when @verbose
      (println "sente conduit transmitting" (pr-str args)))
    (apply f args)))

(defrecord SenteConduit [impl split-transmitter bundled-transmitter partial-parse
                         verbose unhandled
                         message-split-threshold encoders
                         parse-callback]
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
                        (partial/wrap-transmit-to-target-bundled
                         uid
                         (maybe-verbose (:send-fn impl))
                         message-split-threshold
                         encoders)
                        #(tools/error-msg "tried to send" %& "with no UID"))
                      :cljs
                      (partial/wrap-transmit-bundled
                       (maybe-verbose (:send-fn impl))
                       message-split-threshold
                       encoders))
          combined (partial-parse contents)
          result (if (= combined :partial/consumed)
                   :partial/consumed
                   {:routing routing
                    :contents combined
                    :transmit transmit
                    :uid (:uid message)})]
      (when parse-callback
        (parse-callback result))
      result))
  (unhandled [this message provided]
    (unhandled message provided)))

(defn new-sente-conduit
  [{:keys [impl verbose unhandled message-split-threshold
           encoders decoders parse-callback] :as opts}]
  (map->SenteConduit
   (assoc opts
          :partial-parse (partial/wrap-parser-result decoders))))
