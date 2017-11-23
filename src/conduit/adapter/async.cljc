(ns conduit.adapter.async
  (:require [clojure.core.async :as >]
            [taoensso.timbre :as timbre]))

;; TODO - this seems to be doing everything properly except
;; for the part where it sends to the channel?
(def debug (atom []))

(defn get-message-payload
  [decode {:keys [message] :as context}]
  (swap! debug conj {:step ::get-message-payload
                     :context context})
  (assoc context :payload (decode message)))

(defn maybe-send-result
  "returns work-chan if the message is not for us
   returns false if there was a message propagated"
  [capacity-chan {:keys [payload work-chan ignore?] :as context}]
  (swap! debug conj {:step ::maybe-send-result
                     :capacity-chan capacity-chan
                     :context context})
  (if ignore?
    work-chan
    (do (>/put! work-chan payload)
        false)))

(defn check-ignore
  [my-id
   {{:keys [to]} :payload
    :as context}]
  (swap! debug conj {:step ::check-ignore
                     :context context})
  (assoc context :ignore? (and to
                               (not= my-id to))))

(defn make-routing-receiver
  [{:keys [capacity-chan my-id] :as opts}
   {:keys [decode get-message-from-stream message-iterator] :as facilities}]
  (future-call
   (fn async-routing-receiver
     ([]
      (async-routing-receiver false))
     ([send-chan]
      ;; if target-chan is false, we get a new one, otherwise reuse it
      (let [target-chan (or send-chan
                            (and capacity-chan
                                 (>/<!! capacity-chan)))
            maybe-sent (try (some->> target-chan
                                     (get-message-from-stream message-iterator)
                                     (get-message-payload decode)
                                     (check-ignore my-id)
                                     (maybe-send-result capacity-chan))
                            (catch Exception e
                              (timbre/error ::async-routing-receiver (pr-str e))
                              nil))]
        (swap! debug conj {:step ::async-routing-receiver
                           :maybe-sent maybe-sent})
        ;; if nothing in maybe-sent returned nil, recur
        (some-> maybe-sent
                (recur))))))
  (fn submit-to-router []
    (let [res-chan (>/chan)]
      (>/put! capacity-chan res-chan)
      res-chan)))
