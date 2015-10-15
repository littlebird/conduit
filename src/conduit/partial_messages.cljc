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
  [message-string message-id part parts limit]
  (let [a (* part limit)
        b (min (count message-string) (+ a limit))]
    {:message-id message-id
     :part part
     :n-parts parts
     :fragment (subs message-string a b)}))

(defn wrap-transmit*
  [arg-parse transmit-function limit encoders]
  (fn [& args]
    (let [[routing message] (arg-parse args)]
      (if (nil? message)
        (println "nil message in wrap transmit"))
      (if (small-enough? limit message)
        (transmit-function routing message)
        (let [message-id (rand-int #?(:clj
                                      Integer/MAX_VALUE
                                      :cljs
                                      2147483647))
              packed (str (tools/transit-pack message encoders))
              n-parts (int (Math/ceil (/ (count packed) (double limit))))]
          (dotimes [i n-parts]
            (transmit-function routing
                               {:conduit/partial-message (format-part packed message-id i n-parts
                                                                      limit)})))))))

(defn wrap-transmit-separate
  [transmit-function limit encoders]
  (wrap-transmit* identity transmit-function limit encoders))

(defn wrap-transmit-bundled
  [transmit-function limit encoders]
  (wrap-transmit* first
                  (fn [routing data] (transmit-function [routing data]))
                  limit
                  encoders))

(defn is-partial?
  [message]
  (= [:conduit/partial-message] (keys message)))

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
           (if-not (= n-parts (count (get new-state message-id)))
             new-state
             (let [message-parts (get new-state message-id)
                   sorted (sort message-parts)
                   fragments (map (comp :fragment val) sorted)
                   data (apply str fragments)
                   parsed (tools/transit-unpack data decoders)]
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
          (handle-partial message partial-messages decoders))))))
