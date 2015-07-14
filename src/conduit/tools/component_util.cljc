(ns conduit.tools.component-util)

(def side #?(:clj "server" :cljs "client"))

(def stop-words {:present "stopping"
                 :past "stopped"})

(def start-words {:present "starting"
                  :past "started"})

(defn idempotent
  [{:keys [owner name cnd verbs component]} thunk]
  (if cnd
    (let [_ (println owner
                     side
                     (:present verbs)
                     (str name "."))
          result (thunk)]
      result)
    (do
      (println owner
               side
               "not" (:present verbs)
               (str name ",")
               "already" (:past verbs))
      component)))

(defn start
  [component k owner f]
  (idempotent {:verbs start-words
               :owner owner
               :name (name k)
               :component component
               :cnd (not (contains? component k))}
              f))

(defn stop
  [component k owner f]
  (idempotent {:verbs stop-words
               :owner owner
               :name (name k)
               :component component
               :cnd (contains? component k)}
              f))
