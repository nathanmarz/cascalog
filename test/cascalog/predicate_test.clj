(ns cascalog.predicate-test
  (:use clojure.test
        cascalog.testing
        cascalog.predicate)
  (:require [cascalog [workflow :as w]]))

; (defstruct generator :sources :assembly)
; ;;  type is one of :operation :aggregator
; (defstruct operation-predicate :type :assembly :infields :outfields)
; ;;  type is :generator
; (defstruct generator-predicate :type :sources :assembly :outfields)
; (defstruct predicate-variables :in :out)

; (defn build-predicate
;   "Build a predicate. Calls down to build-predicate-specific for predicate-specific building 
;   and adds constant substitution and null checking of ? vars."
;   [op opvar & variables-args]

(w/defmapop timesplusone "blahfield" [a b]
  (inc (* a b)))

(w/defmapop addplusone "blah" [& all]
  [(inc (apply + all)) (first all)])

(deftest test-map-pred
  (let [pred (build-predicate timesplusone (var timesplusone) "?f1" "?f2" :> "?q")
        source-data {:fields ["?a" "?b" "?f1" "?f2" "?c"] :tuples [[1 2 1 1 10]
                                                                    [0 0 2 6 9]
                                                                    [0 0 9 1 0]]}
        sink-data   {:fields ["?q"] :tuples [[2] [13] [10]]} ]
    (is (= :operation (:type pred)))
    (is (= ["?f1" "?f2"] (:infields pred)))
    (is (= ["?q"] (:outfields pred)))
    (test-assembly source-data sink-data (:assembly pred))
    ))

(deftest test-map-constants
  (let [pred (build-predicate addplusone (var addplusone) "?f1" "?f2" 3 4 "?f3" :> "?s" 6)
        source-data {:fields ["?f1" "?f2" "?f3"] :tuples [[6 2 3]
                                                          [8 12 19]
                                                          [6 7 12]
                                                          [1 4 8]]}
        sink-data   {:fields ["?s"] :tuples [[33] [19]]} ]
    (is (= :operation (:type pred)))
    (is (= ["?f1" "?f2" "?f3"] (:infields pred)))
    (is (contains? (set (:outfields pred)) "?s"))
    (is (= 5 (count (:outfields pred))))
    (test-assembly source-data sink-data (:assembly pred))
    ))