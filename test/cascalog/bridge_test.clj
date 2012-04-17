(ns cascalog.bridge-test
  (:use clojure.test)
  (:require [cascalog.workflow :as w]
            [cascalog.testing :as t]
            [cascalog.util :as u])
  (:import [cascading.tuple Fields]
           [cascading.pipe Pipe]
           [cascalog ClojureFilter ClojureMap ClojureMapcat
            ClojureAggregator Util]))

(def obj-array-class
  (Class/forName "[Ljava.lang.Object;"))

(defn plus-one [in]
  [(+ in 1)])

(defn plus-n [n]
  (fn [in]
    [(+ in n)]))

(defn inc-wrapped [num]
  [(inc num)])

(defn inc-both [num1 num2]
  [(inc num1) (inc num2)])


(deftest field-tests
  (let [f1 (w/fields "foo")
        f2 (w/fields ["foo" "bar"])]
    (is (instance? Fields f1))
    (is (= ["foo"] (seq f1)))
    
    (is (instance? Fields f2))
    (is (= ["foo" "bar"] (seq f2)))))

(deftest pipe-testing
  (let [p1 (w/pipe)
        p2 (w/pipe "name")]
    (is (instance? Pipe p1))
    (is (instance? Pipe p2))
    (is (= 36 (.length (.getName p1))))
    (is (= "name") (.getName p2))
    ))


(def clojure-filter-test
  (let [fil (ClojureFilter. odd?)]
    (is (= false (t/invoke-filter fil [1])))
    (is (t/invoke-filter fil [2]))))

(deftest clojure-map-single-field
  (is (= [[2]] (t/invoke-function (ClojureMap. (w/fields "num")
                                    inc-wrapped)
                                  [1])))
  (is (= [[2]] (t/invoke-function (ClojureMap. (w/fields "num")
                                    inc)
                                  [1]))))

(deftest clojure-map-multiple-fields
  (let [m (ClojureMap. (w/fields ["num1" "num2"])
                       inc-both)]
    (is (= [[2 3]] (t/invoke-function m [1 2])))))

(defn iterate-inc-wrapped [num]
  (list [(+ num 1)]
        [(+ num 2)]
        [(+ num 3)]))

(defn iterate-inc [num]
  (list (+ num 1)
        (+ num 2)
        (+ num 3)))

(deftest clojure-mapcat-single
  (is (= [[2] [3] [4]] (t/invoke-function
                                  (ClojureMapcat. (w/fields "num")
                                    iterate-inc-wrapped)
                                  [1])))
  (is (= [[2] [3] [4]] (t/invoke-function
                                  (ClojureMapcat. (w/fields "num")
                                    iterate-inc)
                                  [1]))))

(defn sum
  ([] 0)
  ([mem v] (+ mem v))
  ([mem] [mem]))

(deftest clojure-agg-test
  (let [a (ClojureAggregator. (w/fields "sum")
                              sum)]
    (is (= [[6]] (t/invoke-aggregator a [[1] [2] [3]])))))
