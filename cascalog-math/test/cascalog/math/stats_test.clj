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
  (let [source [[0]]]
    (<- [!var] (source !val) (variance :< !val :> !var))) =>
    (produces [[0.0]]))

(fact ""
  (let [source [[0] [1]]]
    (<- [!var] (source !val) (variance :< !val :> !var))) =>
    (produces [[0.25]]))

(fact "variance is numerically unstable, resulting in the wrong answer"
  (let [n 100
        lo 1000000000
        hi (+ 1 lo)
        source (incanter.stats/sample-uniform n :min lo :max hi)]
    (<- [!var] (source !val) (variance :< !val :> !var))) =>
    (produces [[0.0]]))

;; sample-variance-parallel
(fact ""
  (let [source [[0]]]
    (<- [!var] (source !val) (sample-variance-parallel :< !val :> !var))) =>
    (produces [[0.0]]))

(fact ""
  (let [source [[0] [1]]]
    (<- [!var] (source !val) (sample-variance-parallel :< !val :> !var))) =>
    (produces [[0.5]]))

(fact "variance-parallel is stable, resulting in a much less wrong answer"
  (let [n 100
        lo 1000000000
        hi (+ 1 lo)
        source (incanter.stats/sample-uniform n :min lo :max hi)]
    (<- [!var] (source !val) (sample-variance-parallel :< !val :> !var))) =>
    (produces [[0.0]]))
