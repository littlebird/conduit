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


(defn socket-loop
  [conduit provided shutdown dispatch]
  {:pre [shutdown]} ; shutdown should exist
  (>/go
    (loop []
      (let [socket (conduit/receiver conduit)
            _ (assert socket)
            [message from] (>/alts! [shutdown
                                     (conduit/receiver conduit)])]
        (if (or (not message)
                (= from shutdown))
          (tools/debug-msg (str (conduit/identifier conduit)
                                " conduit socket-loop shutting down"))
          (do (try
                (dispatch message provided)
                (catch
                    #?(:clj Exception
                       :cljs js/Object)
                  e
                  (tools/error-msg (str (conduit/identifier conduit)
                                        " socket-loop uncaught exception"
                                        (pr-str {:error e
                                                 :message message})))))
              (recur)))))))

(defn dispatcher
  [conduit routes]
  (fn
    [msg provided]
    (let [{:keys [routing contents transmit] :as message} (conduit/parse conduit msg)
          unhandled (partial conduit/unhandled conduit)
          handler (get routes routing unhandled)
          provided (assoc provided :transmit transmit :routing routing)]
      (when (conduit/verbose? conduit)
        (tools/debug-msg (str (conduit/identifier conduit)
                              " routing from " routing " with handler " handler
                              (when (= handler unhandled) ", unhandled"))))
      (if (socket-async/out-channel? handler)
        (>/go (>/>! handler [contents provided]))
        (handler contents provided)))))

(defn run-router
  [provided shutdown & [parallelism]]
  {:pre [(:impl provided)
         (:routes provided)]}
  (dotimes [i (or parallelism 1)]
    (socket-loop (:impl provided)
                 (dissoc provided :routes :impl)
                 (>/tap shutdown (>/chan))
                 (dispatcher (:impl provided) (:routes provided)))))

(defn stop-router
  [& args]
  ;; TBI
  )
