(ns conduit.partial-messages
  #?(:clj
     (:require [conduit.tools :as tools]
               [clojure.core.async :as >])
     :cljs
     (:require [conduit.tools :as tools]
               [cljs.core.async :as >]))
  #?(:cljs (:require-macros [cljs.core.async.macros :as >])))

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
  [message-raw message-string message-id part parts limit]
  (let [a (* part limit)
        b (min (count message-string) (+ a limit))]
    {:message-id message-id
     :part part
     :n-parts parts
     :user-data (:user-data message-raw)
     :journey (:journey message-raw)
     :destination (:destination message-raw)
     :creator (:creator message-raw)
     :fragment (subs message-string a b)}))

(defn gen-id
  []
  (rand-int #?(:clj Integer/MAX_VALUE :cljs 2147483647)))

(defn perhaps-send-partial
  [message limit encoders transmit-wrapper]
  (if (small-enough? limit message)
        (transmit-wrapper message)
        (let [message-id (gen-id)
              packed (tools/packup-for-split message encoders)
              n-parts (int (Math/ceil (/ (count packed) (double limit))))]
          (dotimes [i n-parts]
            ;; (println "sending partial" i "of" n-parts)
            (let [package {:conduit/partial-message
                           (format-part message packed message-id i n-parts limit)}]
              (transmit-wrapper package))))))

(defn wrap-transmit-bundled
  [transmit-function limit encoders]
  (fn [[routing message]]
    (perhaps-send-partial
     message
     limit
     encoders
     (fn [data]
       (transmit-function [routing data])))))

(defn wrap-transmit-to-target-bundled
  [destination transmit-function limit encoders]
  (fn [[routing message]]
    (perhaps-send-partial
     message
     limit
     encoders
     (fn [data]
       (transmit-function destination [routing data])))))

(defn wrap-transmit-separate
  [transmit-function limit encoders]
  (fn [destination routing message]
    (perhaps-send-partial
     message
     limit
     encoders
     (fn [data]
       (transmit-function destination routing data)))))

(defn is-partial?
  [message]
  (contains? message :conduit/partial-message))

(defn placeholder
  []
  #?(:clj
     (promise)
     :cljs
     (atom nil)))

(defn fill-place
  [holder value]
  #?(:clj
     (deliver holder value)
     :cljs
     (reset! holder value)))

(defn when-offered
  [holder alternate]
  #?(:clj
     (if (realized? holder)
       @holder
       alternate)
     :cljs
     (if (some? @holder)
       @holder
       alternate)))

(defn handle-partial
  [partial-message partial-messages decoders]
  (let [{{:keys [message-id part n-parts fragment] :as message}
         :conduit/partial-message} partial-message
        constructed (placeholder)]
    (swap!
       partial-messages
       (fn [partials]
         (let [new-state (assoc-in partials [message-id part] message)]
           #_
           (println "Partial message" (pr-str {:message-id message-id
                                               :part (inc part)
                                               :of n-parts}))
           (if-not (= n-parts (count (get new-state message-id)))
             new-state
             (let [message-parts (get new-state message-id)
                   sorted (sort message-parts)
                   fragments (map (comp :fragment val) sorted)
                   data (apply str fragments)
                   parsed (tools/unpack-decode-joined data decoders)]
               (fill-place constructed parsed)
               (dissoc new-state message-id))))))
    (when-offered constructed :partial/consumed)))

(defn wrap-parser-result
  [decoders]
  (let [partial-messages (atom {})]
    (fn [message]
      (when message
        (if-not (and (map? message)
                     (is-partial? message))
          message
          ;; recur here because a partial result can be split!
          (recur
           (handle-partial message partial-messages decoders)))))))
