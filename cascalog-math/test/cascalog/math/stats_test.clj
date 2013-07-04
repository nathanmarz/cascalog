(ns cascalog.math.stats-test
  (:use [cascalog.math.stats]
        [cascalog.api]
        [clojure.test]
        [midje sweet cascalog])
  (:require [incanter.stats]))

;; TODO add test
(fact
  (<- [?x] ([[1]] ?x)) => (produces [[1]]))

;; variance
(fact ""
  (let [source [[0] [0]]]
    (<- [!var] (source !val) (variance :< !val :> !var))) =>
    (produces [[0.0]]))

(fact ""
  (let [n 100
        lo 1000000000
        hi (+ 1 lo)
        source (incanter.stats/sample-uniform n :min lo :max hi)]
    (<- [!var] (source !val) (variance :< !val :> !var))) =>
    (produces [[0.0]]))

;; variance
(fact ""
  (let [n 100
        lo 1000000000
        hi (+ 1 lo)
        source (incanter.stats/sample-uniform n :min lo :max hi)]
    (<- [!var] (source !val) (variance-parallel :< !val :> !var))) =>
    (produces [[0.0]]))
