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

;; need to rename pipes coming from same source
(deftest self-join-test
  (let [source-data {:fields ["a" "b"] :tuples [["a" 1] ["b" 2] ["a" 3] ["c" 4]]}
        sink-data   {:fields ["a" "s" "c"] :tuples [["a" 4.0 2] ["a" 4.0 4] ["b" 2.0 3] ["c" 4.0 5]]}
        assembly    (w/assembly [p] [p1 (p (w/pipe-name "1") (w/group-by "a") (w/sum "b" "s"))
                                     p2 (p (w/pipe-name "2") (w/map #'inc "b" :fn> "c" :> ["a" "c"]))]
                                  ([p1 p2] (w/inner-join ["a" "a"] ["a" "s" "a2" "c"])
                                           (w/select ["a" "s" "c"])))]
        (test-assembly source-data sink-data assembly)))

; (deftest self-only-join-test
;   (let [source-data {:fields ["k" "v"] :tuples [["a" 1] ["b" 2] ["a" 3]]}
;         sink-data   {:fields ["k" "v" "v2"] :tuples [["a" 1 1] ["a" 3 1] ["a" 1 3] ["a" 3 3] ["b" 2 2]]}
;         assembly   (w/assembly [p] [p1 (p (w/pipe-name "1")
;                                           (w/group-by ["k" "v"]))  ; take out group-by and doesn't work
;                                      p2 (p (w/pipe-name "2"))]
;                                      ([p1 p2] (w/inner-join ["k" "k"] ["k" "v" "k2" "v2"])
;                                               (w/select ["k" "v" "v2"])))]
;         (test-assembly source-data sink-data assembly)))

;; conclusion: pipe joining against self with no reduces in branches gives undefined behavior
;; TODO: can I join against something else and then rejoin against source?

;; need the extra group-by in that second branch
; (deftest self-join-after-agg-test
;     (let [source-data {:fields ["a" "b"] :tuples [["a" 1] ["b" 2] ["a" 3] ["c" 4]]}
;           sink-data   {:fields ["a" "s" "z"] :tuples [["a" 4.0 5.0] ["b" 2.0 3.0] ["c" 4.0 5.0]]}
;           assembly    (w/assembly [p] [p (p (w/pipe-name "1") (w/group-by "a") (w/sum "b" "s") (w/debug))
;                                        p2 (p (w/pipe-name "2") (w/group-by ["a" "s"]) (w/debug) (w/map #'inc "s" :fn> "z" :> ["a" "z"]) (w/debug))]
;                                   ([p p2] (w/inner-join ["a" "a"] ["a" "s" "a2" "z"])
;                                           (w/select ["a" "s" "z"])))]
;         (test-assembly :info source-data sink-data assembly)))