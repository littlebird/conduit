(ns conduit.adapter.async-test
  (:require [clojure.test :refer [deftest testing is]]
            [clojure.core.async :as >]
            [conduit.adapter.async :as async]))

(deftest make-routing-receiver-test
  (let [capacity-chan (>/chan)
        my-id ::id]
    (letfn [(decode [x] x)
            (message-iterator
              []
              {::message "hello"})
            (get-message-from-stream
              [message-iterator work-chan]
              {:message (message-iterator)
               :work-chan work-chan})]
      (let [reciever (async/make-routing-receiver {:capacity-chan capacity-chan
                                                   :my-id my-id}
                                                  {:decode decode
                                                   :get-message-from-stream get-message-from-stream
                                                   :message-iterator message-iterator})
            rc-chan (reciever)
            time-out (>/timeout 1000)
            [response rc] (>/alts!! [rc-chan time-out])
            rc-chan-2 (reciever)
            time-out-2 (>/timeout 1000)
            [response-2 rc2] (>/alts!! [rc-chan-2 time-out-2])]
        (is (not= rc time-out))
        (is (= response {::message "hello"}))
        (is (not= rc2 time-out-2))
        (is (= response-2 {::message "hello"}))))))
