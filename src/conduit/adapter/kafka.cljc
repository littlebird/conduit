(ns conduit.adapter.kafka
  (:require [conduit.protocol :as conduit]
            [conduit.tools :as tools]))

(defrecord KafkaConduit [impl verbose unhandled]
  conduit/Conduit
  (identifier [this]
    "kafka conduit channel")
  (verbose? [this]
    (some-> verbose deref))
  (receiver [this]
    ;; core.async channel from config...
    )
  (parse [this message]
    ;; get the routing, contents, response function from the message / instance
    {:routing nil
     :contents nil
     :transmit nil})
  (unhandled [this message provided]
    (unhandled message)))
