(ns conduit.router
  #?@(:cljs
      [(:require-macros [cljs.core.async.macros :as >])
       (:require  [conduit.tools :as tools]
                  [conduit.protocol :as conduit]
                  [conduit.tools.async :as socket-async]
                  [cljs.core.async :as >])]
      :clj
      [(:require [conduit.tools :as tools]
                 [conduit.protocol :as conduit]
                 [conduit.tools.async :as socket-async]
                 [clojure.core.async :as >])]))

(defn log-error-here
  [conduit data e]
  (tools/error-msg
   (str (conduit/identifier conduit)
        " socket-loop uncaught exception"
        (pr-str data)
        \newline
        (pr-str e))))

(defn socket-loop
  [conduit provided shutdown dispatch]
  {:pre [shutdown]}
  (let [done (delay :OK)]
    (>/go
     (loop []
       (when-not (realized? done)
         (let [messagep (volatile! ::unset)]
           (try
            (let [socket (conduit/receiver conduit)
                  [message from] (>/alts! [shutdown socket])]
              (vreset! messagep message)
              (if (or (not message)
                      (= from shutdown))
                (force done)
                #?(:clj
                   (>/<! (>/thread (dispatch message provided)))
                   :cljs
                   (dispatch message provided))))
            (catch #?(:clj Exception :cljs js/Object)
              e
              (log-error-here
               conduit
               {:message @messagep}
               e))
            (catch #?(:clj Throwable :cljs js/Error)
              t
              (log-error-here
               conduit
               {:message @messagep}
               t)
              (throw t)))))
       (if (realized? done)
         (tools/debug-msg (str ::socket-loop
                               " "
                               (conduit/identifier conduit)
                               " conduit socket-loop shutting down"))
         (recur))))))

(defn dispatcher
  [conduit routes]
  (fn
    [msg provided]
    (let [{:keys [routing contents transmit] :as message} (conduit/parse conduit msg)
          unhandled (partial conduit/unhandled conduit)
          handler (get routes routing unhandled)
          provided (assoc provided
                          :transmit transmit
                          :routing routing
                          :message msg)]
      (when (conduit/verbose? conduit)
        (tools/debug-msg (str (conduit/identifier conduit)
                              " routing from " routing " with handler " handler
                              (when (= handler unhandled) ", unhandled"))))
      (if (socket-async/out-channel? handler)
        (>/put! handler [contents provided])
        (try (handler contents provided)
             (catch #?(:clj Exception :cljs js/Object)
               e
               (log-error-here conduit
                               {:routing routing
                                :contents contents}
                               e))
             (catch #?(:clj Throwable :cljs js/Error)
               t
               (log-error-here conduit
                               {:routing routing
                                :contents contents}
                               t)
               (throw t)))))))

(defn run-router
  [provided shutdown parallelism]
  {:pre [(:impl provided)
         (:routes provided)]}
  (dotimes [i parallelism]
    (socket-loop (:impl provided)
                 (dissoc provided :routes :impl)
                 (>/tap shutdown (>/chan))
                 (dispatcher (:impl provided) (:routes provided)))))
