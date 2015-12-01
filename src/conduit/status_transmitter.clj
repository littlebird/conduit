(ns conduit.status-transmitter
  (:require [conduit.kafka :as kafka]
            [noisesmith.component :as component]
            [conduit.tools.component-util :as util])
  (:import (java.util.concurrent ScheduledThreadPoolExecutor TimeUnit)
           (java.util Date)
           (java.net InetAddress)))

(defn generate-status
  ([] (generate-status {}))
  ([static]
   (let [runtime (Runtime/getRuntime)
         unused (.freeMemory runtime)
         limit (.maxMemory runtime)
         allocated (.totalMemory (Runtime/getRuntime))
         used (- allocated unused)
         breathing-room (- limit used)
         summary (str (int (* 100 (/ (double used) limit))) \%)
         stack-traces
         (map #(list (str (key %))
                     (clojure.string/join \newline (val %)))
              (Thread/getAllStackTraces))]
     (merge
      {:memory {:summary summary
                :unused unused
                :limit limit
                :allocated allocated
                :used used
                :breathing-room breathing-room}
       :stacks stack-traces
       :time (Date.)
       :host (str (InetAddress/getLocalHost))}
      static))))

(defn create-thread-executor
  [size]
  (let [executor (ScheduledThreadPoolExecutor. size)]
    (fn schedule-task
      [ms f]
      (let [task (.scheduleWithFixedDelay executor f ms ms TimeUnit/MILLISECONDS)]
        #(.cancel task true)))))

(defrecord KafkaStatus [owner topic register status]
  component/Lifecycle
  (start [component]
    (util/start
     component
     :kafka-status-logger
     owner
     (fn []
       (try
         (assert (-> component :config :config :kafka :zk-host) "must specify host")
         (let [config (-> component :config :config :kafka)
               config (merge
                       {:zk-port 2181
                        :kafka-port 9092}
                       config)
               producer (kafka/make-producer (str (:zk-host config) \:
                                                  (:kafka-port config))
                                             (:producer-opts config))
               transmitter (kafka/encoded-transmitter producer {})
               process-handle (register (fn kafka-status-logger
                                          []
                                          (transmitter topic (status))))]
           (assoc component :stop process-handle))
         (catch Exception e (println "error starting kafka status logger" e)
                (throw e))))))
  (stop [component]
    (util/stop
     component
     :kafka-peer
     owner
     (fn []
       ((:stop component))
       (dissoc component :stop)))))

(defn new-kafka-status
  [{:keys [frequency owner status topic executor] :as opts}]
  (let [static {:owner owner}
        frequency (or frequency 15000)
        executor (or executor (create-thread-executor 1))]
    (map->KafkaStatus {:owner owner :topic topic :register executor :status get-status})))
