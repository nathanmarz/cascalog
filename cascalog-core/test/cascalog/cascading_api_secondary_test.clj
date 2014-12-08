(ns cascalog.cascading-api-secondary-test
    (:use clojure.test
          cascalog.api
          cascalog.logic.testing
          cascalog.cascading.testing)
    (:import [cascading.tuple Fields])
    (:require [cascalog.logic.ops :as c]
              [cascalog.cascading.io :as io]))

(use-fixtures :once
  (fn  [f]
    (set-cascading-platform!)
    (f)))


(deftest test-outfields-tap
  (is (thrown? AssertionError
               (get-out-fields (memory-source-tap Fields/ALL []))))
  (is (= ["!age"]
         (get-out-fields (memory-source-tap ["!age"] []))))
  (is (= ["?age" "field2"]
         (get-out-fields (memory-source-tap ["?age" "field2"] [])))))

;; to test later
(deftest test-negation
  (let [age [["nathan" 25] ["nathan" 24]
             ["alice" 23] ["george" 31]]
        gender [["nathan" "m"] ["emily" "f"]
                ["george" "m"] ["bob" "m"]]
        follows [["nathan" "bob"] ["nathan" "alice"]
                 ["alice" "nathan"] ["alice" "jim"]
                 ["bob" "nathan"]]]
    (test?<- [["george"]]
             [?p]
             (age ?p _)
             (follows ?p _ :> false))

    (test?<- [["nathan"] ["nathan"]
              ["alice"]]
             [?p]
             (age ?p _)
             (follows ?p _ :> true))

    (test?<- [["alice"]]
             [?p]
             (age ?p _)
             (follows ?p "nathan" :> true))

    (test?<- [["nathan"] ["nathan"]
              ["george"]]
             [?p]
             (age ?p _)
             (follows ?p "nathan" :> false))

    (test?<- [["nathan" true true] ["nathan" true true]
              ["alice" true false] ["george" false true]]
             [?p ?isfollows ?ismale]
             (age ?p _)
             (follows ?p _ :> ?isfollows)
             (gender ?p "m" :> ?ismale))

    (test?<- [["nathan" true true]
              ["nathan" true true]]
             [?p ?isfollows ?ismale]
             (age ?p _)
             (follows ?p _ :> ?isfollows)
             (gender ?p "m" :> ?ismale)
             (= ?ismale ?isfollows))

    (let [old (<- [?p ?a]
                  (age ?p ?a)
                  (> ?a 30))]
      (test?<- [["nathan"] ["bob"]]
               [?p]
               (gender ?p "m")
               (old ?p _ :> false)))

    (test?<- [[24] [31]]
             [?n]
             (age _ ?n)
             ([[25] [23]] ?n :> false))

    (test?<- [["alice"]]
             [?p]
             (age ?p _)
             ((c/negate gender) ?p _))))

;; currently fails
(deftest test-negation-operations
  (let [nums [[1] [2] [3] [4]]
        pairs [[3 4] [4 5]]]
    (test?<- [[1] [2] [3]]
             [?n]
             (nums ?n)
             (pairs ?n ?n2 :> false)
             (odd? ?n2))))

;; currently passes but uses cascading
(deftest test-first-n
  (let [sq (name-vars [[1 1] [1 3] [1 2] [2 1] [3 4]]
                      ["?a" "?b"])]
    (test?- [[1 1] [1 2]]
            (c/first-n sq 2 :sort ["?a" "?b"]))
    (test?- [[3 4] [2 1]]
            (c/first-n sq 2 :sort "?a" :reverse true))
    (is (= 2 (count (first (??- (c/first-n sq 2))))))))
