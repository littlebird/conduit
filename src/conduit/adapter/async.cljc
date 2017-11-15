(ns conduit.adapter.async
  (:require [clojure.core.async :as >]
            [taoensso.timbre :as timbre]))

(def debug (atom []))

(defn get-message-payload
  [decode {:keys [message] :as context}]
  (assoc context :payload (decode message)))

(defn maybe-send-result
  [capacity-chan {:keys [payload work-chan ignore?]}]
  (swap! debug conj {:context ::maybe-send-result
                     :payload payload
                     :capacity-chan capacity-chan
                     :work-chan work-chan})
  (timbre/debug ::make-async-routing-receiver$maybe-send-result
                "checking out message"
                (pr-str {:ignore? ignore?
                         :payload payload}))
  (if ignore?
    work-chan
    (do (timbre/debug ::make-asnc-routing-receiver$maybe-send-result
                      "handling message for chan" work-chan)
        (and (>/put! work-chan payload)
             nil))))

(defn check-ignore
  [my-id
   {{:keys [to]} :payload
    :as context}]
  (assoc context :ignore? (and to
                               (not= my-id to))))
(defn make-routing-receiver
  [{:keys [capacity-chan my-id] :as opts}
   {:keys [decode get-message-from-stream message-iterator] :as facilities}]
  (future-call
   (fn async-routing-receiver
     ([]
      (timbre/debug ::async-routing-receiver
                    "making a receiver with no fixed send-chan")
      (async-routing-receiver false))
     ([send-chan]
      ;; if target-chan is false, we get a new one, otherwise reuse it
      (timbre/debug ::async-routing-receiver "getting next message")
      (let [target-chan (or send-chan
                            (and capacity-chan
                                 (>/<!! capacity-chan)))
            _ (timbre/debug ::async-routing-receiver
                            "got a target chan"
                            target-chan)
            maybe-sent (try (some->> target-chan
                                     (get-message-from-stream message-iterator)
                                     (get-message-payload decode)
                                     (check-ignore my-id)
                                     (maybe-send-result capacity-chan))
                            (catch Exception e
                              (timbre/error ::async-routing-receiver (pr-str e))
                              nil))]
        ;; if nothing in maybe-sent returned nil,
        ;; recur with the next channel to use
        (timbre/debug ::async-routing-receiver "got a message."
                      "ignore?" (boolean maybe-sent))
        (some-> maybe-sent
                (recur))))))
  (fn []
    (let [res-chan (>/chan)]
      (swap! debug conj {:context ::async-routing-receiver
                         :capacity-chan capacity-chan
                         :res-chan res-chan})
      (>/put! capacity-chan res-chan)
      res-chan)))
