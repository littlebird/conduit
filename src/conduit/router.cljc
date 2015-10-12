(ns conduit.router
  #?@(:cljs
      [(:require-macros [cljs.core.async.macros :as >])
       (:require  [conduit.tools :as tools]
                  [conduit.protocol :as conduit]
                  [clojure.string :as string]
                  [conduit.tools.async :as socket-async]
                  [goog.string :as gstring]
                  [goog.string.format]
                  [cljs.core.async :as >])]
      :clj
      [(:require [conduit.tools :as tools]
                 [conduit.protocol :as conduit]
                  [clojure.string :as string]
                 [conduit.tools.async :as socket-async]
                 [clojure.core.async :as >])]))

(def magic-partial-flag "partial message bb167df1-e4dc-4fba-aca2-5e9482d84f16")

(defn small-enough?
  [limit input]
  (reduce (fn [total el]
            (let [result (+ total (count (pr-str el)))]
              (if (> result limit)
                (reduced false)
                result)))
          0
          (remove coll? (tree-seq coll? seq input))))

(defn format-part
  [message-string message-id part parts limit]
  (let [a (* part limit)
        b (min (count message-string) (+ a limit))]
    (#?(:clj format :clj gstring/format)
       "%s%12s%12d%12d%s" magic-partial-flag message-id
        part parts
        (subs message-string a b))))

(defn unformat-part
  [message-string]
  (let [distances [(count magic-partial-flag) 12 12 12]
        points (reductions + distances)]
    {:message-id (-> message-string
                     (subs (nth points 0) (nth points 1))
                     string/trim
                     #?(:clj
                        Integer/parseInt
                        :cljs
                        js/parseInt))
     :part (-> message-string
               (subs (nth points 1) (nth points 2))
               string/trim
               #?(:clj
                  Integer/parseInt
                  :cljs
                  js/parseInt))
     :n-parts (-> message-string
                  (subs (nth points 2) (nth points 3))
                  string/trim
                  #?(:clj
                     Integer/parseInt
                     :cljs
                     js/parseInt))
     :fragment (subs message-string (nth points 3))}))

(defn wrap-transmit
  [transmit limit encoders]
  (fn [routing message]
    (if (small-enough? limit message)
      (transmit routing message)
      (let [message-id (#?(:clj format :clj gstring/format)
                          "%12d" (rand-int #?(:clj
                                              Integer/MAX_VALUE
                                              :cljs
                                              2147483647)))
            packed (str (tools/transit-pack message :json encoders))
            n-parts (int (Math/ceil (/ (count packed) (double limit))))]
        (dotimes [i n-parts]
          (transmit routing (format-part packed message-id i n-parts
                                         limit)))))))

(defn is-partial?
  [^String message]
  (.startsWith message magic-partial-flag))

(defn handle-partial
  [message partial-messages multipart-message-channel decoders]
  (let [{:keys [message-id part n-parts fragment]} (unformat-part message)]
    (#?(:clj send
        :cljs swap!) partial-messages
        (fn [partials]
          (let [new-state (assoc-in partials [message-id part] message)]
            (if-not (= n-parts (count (get new-state message-id)))
              new-state
              (let [data (-> new-state
                             (get message-id)
                             (sort)
                             (->>
                              (map (comp :fragment val))
                              (apply str))
                             (tools/transit-unpack decoders))]
                (>/put! multipart-message-channel data)
                (dissoc new-state message-id))))))))

(defn socket-loop
  [conduit provided shutdown dispatch
   partial-messages multipart-message-channel
   decoders]
  {:pre [shutdown]} ; shutdown should exist
  (>/go
    (loop []
      (let [socket (conduit/receiver conduit)
            _ (assert socket)
            [message from] (>/alts! [shutdown
                                     multipart-message-channel
                                     (conduit/receiver conduit)])]
        (cond (or (not message)
                (= from shutdown))
              (tools/debug-msg (str (conduit/identifier conduit)
                                    " conduit socket-loop shutting down"))
              (is-partial? message)
              (handle-partial message partial-messages
                              multipart-message-channel
                              decoders)
              :default
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
  [conduit routes limit encoders]
  (fn
    [msg provided]
    (let [message (conduit/parse conduit msg)
          {:keys [routing contents transmit]} message
          unhandled (partial conduit/unhandled conduit)
          handler (get routes routing unhandled)
          provided (assoc provided
                          :transmit (wrap-transmit transmit limit encoders)
                          :routing routing)]
      (when (conduit/verbose? conduit)
        (tools/debug-msg (str (conduit/identifier conduit)
                              " routing from " routing " with handler " handler
                              (when (= handler unhandled) ", unhandled"))))
      (if (socket-async/out-channel? handler)
        (>/go (>/>! handler [contents provided]))
        (handler contents provided)))))

(defn run-router
  ([provided shutdown message-size-limit]
   (run-router provided shutdown message-size-limit 1))
  ([provided shutdown message-size-limit parallelism]
   {:pre [(:impl provided)
          (:routes provided)
          (:encoders provided)]}
   (let [partial-messages (#?(:clj agent :cljs atom) {})
         multipart-message-channel (>/chan)]
     (dotimes [i parallelism]
       (socket-loop (:impl provided)
                    (dissoc provided :routes :impl)
                    (>/tap shutdown (>/chan))
                    (dispatcher (:impl provided)
                                (:routes provided)
                                message-size-limit
                                (:encoders provided))
                    partial-messages
                    multipart-message-channel
                    (or (:decoders provided) {}))))))

(defn stop-router
  [& args]
  ;; TBI
  )
