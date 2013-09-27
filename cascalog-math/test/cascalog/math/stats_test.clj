(ns cascalog.math.stats-test
  (:use [cascalog.math.stats]
        [cascalog.api]
        [clojure.test]
        [midje sweet cascalog]))

;; TODO add test
(fact
  (<- [?x] ([[1]] ?x)) => (produces [[1]]))
