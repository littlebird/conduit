(ns conduit.status-transmitter
  (:require [taoensso.timbre :as timbre]
            [conduit.kafka :as kafka]
            [noisesmith.component :as component]
            [conduit.tools.component-util :as util])
  (:import (java.util.concurrent ScheduledThreadPoolExecutor TimeUnit)
           (java.util Date)
           (java.net InetAddress)
           (java.lang.management ManagementFactory)
           (javax.management ObjectName)))

(defn get-cpu
  []
  (let [mbs (ManagementFactory/getPlatformMBeanServer)
        oname (ObjectName/getInstance "java.lang:type=OperatingSystem")
        ls (.getAttributes mbs oname (into-array ["ProcessCpuLoad"]))
        usages-raw (map #(.getValue %) ls)
        usage (map #(/ (* % 1000) 10.0) usages-raw)]
    usage))

(defn get-all-stacks
  []
  (map #(list (str (key %))
              (clojure.string/join \newline (val %)))
       (Thread/getAllStackTraces)))

(defn get-memory-usage
  []
  (let [runtime (Runtime/getRuntime)
        unused (.freeMemory runtime)
        limit (.maxMemory runtime)
        allocated (.totalMemory (Runtime/getRuntime))
        used (- allocated unused)
        breathing-room (- limit used)
        summary (str (int (* 100 (/ (double used) limit))) \%)]
    {:summary summary
     :unused unused
     :limit limit
     :allocated allocated
     :used used
     :breathing-room breathing-room}))

(defn generate-status
  ([] (generate-status {}))
  ([static]
   (merge
    {:memory (get-memory-usage)
     :cpu (get-cpu)
     #_#_:stacks (get-all-stacks) ; add this back in when you want to send stack traces
     :time (Date.)
     :host (str (InetAddress/getLocalHost))}
    static)))

(defn create-thread-executor
  [size]
  (let [executor (ScheduledThreadPoolExecutor. size)]
    (fn schedule-task
      [ms f]
      (let [task (.scheduleWithFixedDelay executor
                                          f ms ms TimeUnit/MILLISECONDS)]
        #(.cancel task true)))))

(defrecord KafkaStatus [owner topic register status kafka-opts]
  component/Lifecycle
  (start [component]
    (util/start
     component
     :kafka-status-logger
     owner
     (fn []
       (try
         (assert (or (get-in component [:config :config :kafka :kafka-connect])
                     (get-in component [:config :config :kafka :zk-host]))
                 "must specify host")
         (let [config (-> component :config :config :kafka)
               config (merge
                       {:zk-port 2181
                        :kafka-port 9092}
                       config
                       kafka-opts)
               connect-string (or (:kafka-connect config)
                                  (str (:zk-host config) \:
                                       (:kafka-port config)))
               producer (kafka/make-producer connect-string
                                             (or (:producer-opts config)
                                                 {}))
               transmitter (kafka/encoded-transmitter producer {})
               process-handle (register
                               (fn kafka-status-logger
                                 []
                                 (try
                                   (transmitter topic (status component))
                                   (catch Exception e
                                     (timbre/error ::kafka-status-logger
                                                   (pr-str e))))))]
           (assoc component :stop process-handle))
         (catch AssertionError e
           (timbre/error ::KafkaStatus$start (pr-str e))
           (throw e))
         (catch Exception e
           (timbre/error ::KafkaStatus$start (pr-str e))
           (throw e))))))
  (stop [component]
    (util/stop
     component
     :kafka-peer
     owner
     (fn []
       ((:stop component))
       (dissoc component :stop)))))

(defn gen-custom
  [state-store]
  (fn gen-custom-stats [_]
    (let [next-state (volatile! nil)]
      (swap! state-store
             (fn [updated]
               (vreset! next-state updated)
               {}))
      {:metrics @next-state})))

(defn tracking-middleware
  [handler get-state key-sequence]
  (fn stat-updater
    [& args]
    (let [state (get-state args)]
      (swap! state update key-sequence (fnil inc 0)))
    (apply handler args)))

(defn new-kafka-status
  [{:keys [frequency owner topic executor custom-status] :as opts}]
  (let [static {:owner owner}
        frequency (or frequency
                      15000)
        executor (or executor
                     (create-thread-executor 1))
        state (atom {})
        custom-status (or custom-status
                          (gen-custom state))
        gen-status #(generate-status (merge static (custom-status %)))]
    (map->KafkaStatus (assoc opts
                             :owner owner
                             :topic topic
                             :register (partial executor frequency)
                             :state state
                             :status gen-status))))
