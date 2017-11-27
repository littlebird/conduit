(ns conduit.adapter.async
  (:require [clojure.core.async :as >]
            [conduit.tools.async :as async]
            [conduit.protocol :as conduit]
            [taoensso.timbre :as timbre])
  (:import (java.util UUID)))

(defrecord AsyncConduit [transmitter receiver-fn verbose unhandled id]
  conduit/Conduit
  (identifier [this]
    "kafka async channel")
  (verbose? [this]
    (some-> verbose deref))
  (receiver [this]
    (let [received (receiver-fn)]
      received))
  (parse [this [routing contents]]
    ;; get the routing, contents, response function from the message / instance
    {:routing routing
     :contents contents
     :transmit transmitter})
  (unhandled [this message provided]
    (unhandled message)))

(defn make-routing-receiver
  "returns a function taking a channel which will receive a message
   the client should act on; if no channel provided it will create and return
   a fresh channel"
  [{:keys [capacity-chan task-chan] :as opts}]
  {:pre [capacity-chan task-chan]}
  (let [facilities {:decode identity
                    :get-message-from-stream (fn [tasks target]
                                               {:message (>/<!! tasks)
                                                :work-chan target})
                    :message-iterator task-chan}]
    (async/make-routing-receiver opts facilities)))

(defn new-async-conduit
  [{:keys [request-chan work-chan id verbose unhandled]}]
  (let [my-id (or id (UUID/randomUUID))
        receiver-args {:my-id my-id
                       :capacity-chan request-chan
                       :task-chan work-chan}
        receiver-fn (make-routing-receiver receiver-args)
        transmitter (fn [to route message]
                      (>/put! work-chan [to route my-id message]))]
    (map->AsyncConduit {:transmitter transmitter
                        :receiver-fn receiver-fn
                        :verbose verbose
                        :unhandled unhandled
                        :id my-id})))
