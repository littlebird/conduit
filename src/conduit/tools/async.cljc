(ns conduit.tools.async
  (:require [conduit.tools :as tools]
            #?@(:cljs
                [[cljs.core.async.impl.protocols :refer [WritePort]]
                 #_[cljs.core.async :as >]]
                :clj
                [[clojure.core.async :as >]]))
  #?@(:clj [(:import (clojure.core.async.impl.protocols WritePort))]))

(defn out-channel?
  [x]
  #?(:clj
     (instance? WritePort x)
     :cljs
     (satisfies? WritePort x)))

(def debug (atom []))

(defn get-message-payload
  [decode {:keys [message] :as context}]
  (swap! debug conj {:step ::get-message-payload
                     :context context})
  (assoc context :payload (decode message)))

;; TODO - make this work with cljs
#?(:clj
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
           false))))

(defn check-ignore
  [my-id
   {{:keys [to]} :payload
    :as context}]
  (swap! debug conj {:step ::check-ignore
                     :context context})
  (assoc context :ignore? (and to
                               (not= my-id to))))

;; TODO - make this work with cljs
#?(:clj
   (defn make-routing-receiver
     [{:keys [capacity-chan my-id] :as opts}
      {:keys [decode get-message-from-stream message-iterator] :as facilities}]
     (>/go
      (loop [send-chan false]
        ;; if target-chan is false, we get a new one, otherwise reuse it
        (let [target-chan (or send-chan
                              (and capacity-chan
                                   (>/<! capacity-chan)))
              maybe-sent (try (some->> target-chan
                                       (get-message-from-stream message-iterator)
                                       (>/<!)
                                       (get-message-payload decode)
                                       (check-ignore my-id)
                                       (maybe-send-result capacity-chan))
                              (catch #?(:clj Exception
                                        :cljs Object)
                                e
                                (tools/error-msg (str ::async-routing-receiver (pr-str e)))
                                nil))]
          (swap! debug conj {:step ::async-routing-receiver
                             :maybe-sent maybe-sent})
          ;; if nothing in maybe-sent returned nil, recur
          (some-> maybe-sent
                  (recur)))))
     (fn submit-to-router
       ([]
        (submit-to-router (>/chan)))
       ([res-chan]
        (>/put! capacity-chan res-chan)
        res-chan))))
