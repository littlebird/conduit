(ns conduit.tools.component-util)

(defn idempotent
  [{:keys [owner name cnd verb component]} thunk]
  (if cnd
    (do (println owner
                 #?(:clj "server" :cljs "client")
                 (str verb "ing") (str name "."))
        (thunk))
    (do
      (println owner
               #?(:clj "server" :cljs "client")
               "not" (str verb "ing")
               (str name ",") "already" (str verb "ed."))
      component)))

(defn start
  [component k owner f]
  (idempotent {:verb "start"
               :owner owner
               :name (name k)
               :component component
               :cnd (not (contains? component k))}
              f))

(defn stop
  [component k owner f]
  (idempotent {:verb "stop"
               :owner owner
               :name (name k)
               :component component
               :cnd (contains? component k)}
              f))
