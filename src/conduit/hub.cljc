(ns conduit.hub
  #?(:cljs (:require-macros [cljs.core.async.macros :as >]))
  (:require [conduit.protocol :as conduit]
            [conduit.tools.component-util :as component-tools]
            [conduit.router :as router]
            [noisesmith.component :as component]
            #?(:clj
               [clojure.core.async :as >]
               :cljs
               [cljs.core.async :as >])))

(defrecord Conduit [owner constructor routes-lookup parallelism
                    message-size-limit]
  component/Lifecycle
  (start [component]
    (component-tools/start
     component
     :conduit
     owner
     (fn []
       (let [shutdown (>/chan)
             invoke-handshake (>/chan)
             handshake (>/mult invoke-handshake)
             after-handshake (fn post-handshake-hook [f]
                               (let [signal (>/tap handshake (>/chan))]
                                 (>/go
                                   (>/<! signal)
                                   (f))))
             provided (-> component :provided :provided)
             impl (constructor component)]
         (println "conduit starting up router for" owner)
         (router/run-router (assoc provided
                                   :routes (get-in component routes-lookup)
                                   :impl impl
                                   :handshake invoke-handshake)
                             (>/mult shutdown)
                             (or message-size-limit 32768)
                             (or parallelism 1))
         (println "conduit router for" owner "started")
         (assoc component
                :impl impl
                :conduit :running
                :shutdown shutdown
                :after-handshake after-handshake)))))
  (stop [component]
    (component-tools/stop
     component
     :conduit
     owner
     (fn []
       (router/stop-router (:impl component)
                           (:provided component)
                           (:routes component))
       (>/go (some-> component :shutdown (>/>! :done)))
       (dissoc component :conduit :after-handshake :shutdown)))))

(defn new-conduit
  [{:keys [owner] :as config}]
  (map->Conduit config))
