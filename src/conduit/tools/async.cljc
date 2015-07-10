(ns conduit.tools.async
  #?@(:clj
      [(:require [clojure.core.async :as >])
       (:import (clojure.core.async.impl.protocols WritePort))]
      :cljs
      [(:require [cljs.core.async :as >]
                 [cljs.core.async.impl.protocols :refer [WritePort]])]))


(defn out-channel?
  [x]
  #?(:clj
     (instance? WritePort x)
     :cljs
     (satisfies? WritePort x)))
