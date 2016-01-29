(ns cascalog.math.stats-test
  (:use [cascalog.math.stats]
        [cascalog.api]
        [clojure.test]
        [midje sweet cascalog])
  (:import (cern.jet.random.tdouble DoubleUniform)))

(background
 (before :facts
         (set-cascading-platform!)))

(defn sample-uniform [size min-val max-val seed]
  (let [dist (DoubleUniform. (double min-val) (double max-val) seed)]
    (for [_ (range size)] (. dist nextDouble))))

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

(fact "variance is numerically unstable, resulting in a very wrong answer"
  (let [n 100
        lo 1000000000
        hi (+ 1 lo)
        seed 1234
        source (sample-uniform n lo hi seed)]
    (<- [!var] (source !val) (variance :< !val :> !var))) =>
    (produces [[256.0]]))

;; sample-variance-parallel
(fact ""
  (let [source [[0]]]
    (<- [!var] (source !val) (sample-variance-parallel :< !val :> !var))) =>
    (produces [[0.0]]))

(fact ""
  (let [source [[0] [1]]]
    (<- [!var] (source !val) (sample-variance-parallel :< !val :> !var))) =>
    (produces [[0.5]]))

(fact "variance-parallel is stable, resulting in nearly the right answer"
  (let [n 100
        lo 1000000000
        hi (+ 1 lo)
        seed 1234
        source (sample-uniform n lo hi seed)]
    (<- [!var] (source !val) (sample-variance-parallel :< !val :> !var))) =>
    (produces [[0.09958331251840505]]))
