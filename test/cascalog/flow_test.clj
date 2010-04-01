(ns cascalog.flow-test
  (:use clojure.test
        cascalog.testing)
  (:import [cascading.tuple Fields])
  (:require [cascalog [workflow :as w]]))

(deftest test-simple-assembly
  (let [source-data {:fields ["a" "b"] :tuples [[2 1] [11 10]]}
        sink-data {:fields ["b" "c"] :tuples [[1 3] [10 12]]}]
    (test-assembly source-data sink-data
      (w/map #'inc "a" :fn> "c" :> ["b" "c"]))
    ))

(deftest test-filter-filter)

(deftest test-filter-map)

(deftest test-join)

(deftest test-buffer)

(deftest test-aggregator)