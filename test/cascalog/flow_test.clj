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
        assembly    (w/assembly [p] [p1 (p (w/group-by "a") (w/sum "b" "s") (w/pipe-name "1"))
                                     p2 (p (w/map #'inc "b" :fn> "c" :> ["a" "c"]) (w/pipe-name "2"))]
                                  ([p1 p2] (w/inner-join ["a" "a"] ["a" "s" "a2" "c"])
                                           (w/select ["a" "s" "c"])))]
        (test-assembly source-data sink-data assembly)))

(deftest disconnected-test
  (let [source-data [{:fields ["a"] :tuples [[1] [2] [3]]}
                      {:fields ["b"] :tuples [[10]]}]
        sink-data   [{:fields ["x"] :tuples [[2] [3] [4]]}
                     {:fields ["y"] :tuples [[9]]}]
        assembly    (w/assembly [p1 p2] [p1 (p1 (w/map #'inc "a" :fn> "x" :> "x"))
                                         p2 (p2 (w/map #'dec "b" :fn> "y" :> "y"))]
                                [p1 p2])]
        (test-assembly source-data sink-data assembly)))

(deftest self-only-join-test
  (let [source-data {:fields ["k" "v"] :tuples [["a" 1] ["b" 2] ["a" 3]]}
        sink-data   {:fields ["k" "v" "v2"] :tuples [["a" 1 1] ["a" 3 1] ["a" 1 3] ["a" 3 3] ["b" 2 2]]}
        assembly   (w/assembly [p] ([p p] (w/inner-join ["k" "k"] ["k" "v" "k2" "v2"])
                                              (w/select ["k" "v" "v2"])))]
        (test-assembly source-data sink-data assembly)))

;; TODO: can I join against something else and then rejoin against source?

(deftest self-join-after-agg-test
    (let [source-data {:fields ["a" "b"] :tuples [["a" 1] ["b" 2] ["a" 3] ["c" 4]]}
          sink-data   {:fields ["a" "s" "z"] :tuples [["a" 4.0 5.0] ["b" 2.0 3.0] ["c" 4.0 5.0]]}
          assembly    (w/assembly [p] [p (p (w/pipe-name "1") (w/group-by "a") (w/sum "b" "s"))
                                       p2 (p (w/pipe-name "2")
                                             (w/map #'inc "s" :fn> "z" :> ["a" "z"]))]
                                  ([p p2] (w/inner-join ["a" "a"] ["a" "s" "a2" "z"])
                                          (w/select ["a" "s" "z"])))]
        (test-assembly source-data sink-data assembly)))