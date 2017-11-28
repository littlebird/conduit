(ns conduit.hub_test
  (:require [clojure.test :as t :refer [deftest testing is]]
            [conduit.hub :as hub]
            [conduit.adapter.async :as impl]
            [noisesmith.component :as component]
            [clojure.core.async :as >]))

(deftest hub-test
  (let [log (atom [])
        rq-chan (>/chan)
        work-chan (>/chan)
        opts {:owner 'hub-test
              :constructor impl/new-async-conduit
              :routes {:foo #(swap! log conj {:route 'foo :args %})}
              :request-chan rq-chan
              :work-chan work-chan
              :id "hub-test-hub"
              :verbose (atom true)
              :unhandled (fn [& un] (swap! log conj {:route 'unhandled :args un}))}
        conduit (component/start (hub/new-conduit opts))]
    (is conduit)))
