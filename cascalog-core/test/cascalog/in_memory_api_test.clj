(ns cascalog.in-memory-api-test
  (:use clojure.test
        [midje sweet cascalog]
        cascalog.logic.testing
        cascalog.in-memory.testing
        cascalog.api)
  (:require [cascalog.in-memory.platform :as p]
            [cascalog.in-memory.tuple :as t]))

(use-fixtures :once
  (fn  [f]
    (set-in-memory-platform!)
    (f)))

(deftest test-atom-sink
  (let [results (atom [])]
    (?<- results [?n] ([[1] [2] [3]] ?n))
    (is (= [{"?n" 1} {"?n" 2} {"?n" 3}]
           @results))))

(deftest test-fn-sink
  (let [results (atom [])]
    (letfn [(reset-atom [tuples fields]
              (reset! results tuples))]
      (?<- reset-atom [?n] ([[1] [2] [3]] ?n))
      (is (= [{"?n" 1} {"?n" 2} {"?n" 3}]
             @results)))))

