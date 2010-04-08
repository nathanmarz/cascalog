(ns cascalog.core-test
  (:use clojure.test
        cascalog.testing
        cascalog.core)
  (:import [cascading.tuple Fields])
  (:require [cascalog [workflow :as w]]))

(deftest test-basic-query
  (with-tmp-sources [data1 {:fields ["1" "2"] :tuples [[1 2] [3 4] [5 nil]]} ]
    (?<- (w/lfs-tap (w/text-line ["a" "b"]) "/tmp/aaa") [?a ?b] (data1 ?a ?b2) (inc ?b2 :> ?b))
   ))