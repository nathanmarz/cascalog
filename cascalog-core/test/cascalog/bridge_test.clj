(ns cascalog.bridge-test
    (:import [cascalog.aggregator ClojureAggregator])
    (:use midje.sweet
        clojure.test)
  (:require [cascalog.fluent.workflow :as w]
            [cascalog.fluent.cascading :refer (fields)]
            [cascalog.fluent.operations :as ops]
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

(defn is-type
  "Accepts a class and returns a checker that tests whether or not its
  input is an instance of the supplied class."
  [^Class expected]
  (chatty-checker
    [actual]
    (instance? expected actual)))

(deftest Fields-test
  (facts "Fields tests."
    (let [f1 (fields "foo")
          f2 (fields ["foo" "bar"])]
      (facts "Single fields should resolve properly."
        f1       => #(instance? Fields %)
        (seq f1) => ["foo"])

      (facts "Double fields should resolve properly."
        f2       => #(instance? Fields %)
        (seq f2) => ["foo" "bar"]))))

(deftest pipe-test
  (tabular
    (fact "Pipe testing."
      ?pipe => #(instance? Pipe %)
      (.getName ?pipe) => ?check)
    ?pipe           ?check
    (w/pipe)        #(= 36 (.length %))
    (w/pipe "name") "name"))

(deftest ClojureFilter-test
  (fact "Clojure Filter test."
    (let [fil (ClojureFilter. (ops/fn-spec #'odd?) false)]
      (t/invoke-filter fil [1]) => false
      (t/invoke-filter fil [2]) => true)))

(deftest ClojureMap-single-field-test
  (tabular
    (fact "ClojureMap test, single field."
      (t/invoke-function ?clj-map [1]) => [[2]])
    ?clj-map
    (ClojureMap. (fields "num")
                 (ops/fn-spec #'inc-wrapped)
                 false)
    (ClojureMap. (fields "num")
                 (ops/fn-spec #'inc)
                 false)))

(deftest ClojureMap-multiple-fields-test
  (facts "ClojureMap test, multiple fields."
    (let [m (ClojureMap. (fields ["num1" "num2"])
                         (ops/fn-spec #'inc-both)
                         false)]
      (t/invoke-function m [1 2]) => [[2 3]])))

(defn iterate-inc-wrapped [num]
  (list [(+ num 1)]
        [(+ num 2)]
        [(+ num 3)]))

(defn iterate-inc [num]
  (list (+ num 1)
        (+ num 2)
        (+ num 3)))

(deftest ClojureMapCat-single-field-test
  (tabular
    (fact "ClojureMapCat test, single field."
      (t/invoke-function ?clj-mapcat [1]) => [[2] [3] [4]])
    ?clj-mapcat
    (ClojureMapcat. (fields "num")
                    (ops/fn-spec #'iterate-inc-wrapped)
                    false)
    (ClojureMapcat. (fields "num")
                    (ops/fn-spec #'iterate-inc)
                    false)))

(defn sum
  ([] 0)
  ([mem v] (+ mem v))
  ([mem] [mem]))

(deftest ClojureAggregator-test
  (fact "ClojureAggregator test."
    (let [a (ClojureAggregator. (fields "sum")
                                (ops/fn-spec #'sum)
                                false)]
      (t/invoke-aggregator a [[1] [2] [3]]) => [[6]])))
