(ns cascalog.math.stats-test
  (:use [cascalog.math.stats]
        [midje sweet cascalog]))

(fact?<- [[1]] [?x] ([[1]] ?x))
