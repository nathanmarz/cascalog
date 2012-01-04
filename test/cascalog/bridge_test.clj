(ns cascalog.bridge-test
  (:use clojure.test)
  (:require [cascalog.workflow :as w]
            [cascalog.testing :as t])
  (:import (cascading.tuple Fields)
           (cascading.pipe Pipe)
           (cascalog ClojureFilter ClojureMap ClojureMapcat
                     ClojureAggregator Util)))

(deftest test-ns-fn-name-pair
  (let [[ns-name fn-name] (w/ns-fn-name-pair #'str)]
    (is (= "clojure.core" ns-name))
    (is (= "str" fn-name))))

(def obj-array-class (class (into-array Object [])))

(defn inc1 [in]
  [(+ in 1)])

(defn incn [n]
  (fn [in] [(+ in n)]))

(defn inc-wrapped [num]
  [(inc num)])

(defn inc-both [num1 num2]
  [(inc num1) (inc num2)])

(deftest test-fn-spec-simple
  (let [fs (w/fn-spec #'inc1)]
    (is (instance? obj-array-class fs))
    (is (= '("cascalog.bridge-test" "inc1") (seq fs)))))

(deftest test-fn-spec-hof
  (let [fs (w/fn-spec [#'incn 3])]
    (is (instance? obj-array-class fs))
    (is (= `("cascalog.bridge-test" "incn" 3) (seq fs)))))

(deftest test-boot-fn-simple
  (let [spec (into-array Object `("cascalog.bridge-test" "inc1"))
        f    (Util/bootFn spec)]
    (is (= [2] (f 1)))))

(deftest test-boot-fn-hof
  (let [spec (into-array Object '("cascalog.bridge-test" "incn" 3))
        f    (Util/bootFn spec)]
    (is (= [4] (f 1)))))

(deftest test-1-field
  (let [f1 (w/fields "foo")]
    (is (instance? Fields f1))
    (is (= '("foo") (seq f1)))))

(deftest test-n-fields
  (let [f2 (w/fields ["foo" "bar"])]
    (is (instance? Fields f2))
    (is (= `("foo" "bar") (seq f2)))))

(deftest test-uuid-pipe
  (let [up (w/pipe)]
    (is (instance? Pipe up))
    (is (= 36 (.length (.getName up))))))

(deftest test-named-pipe
  (let [np (w/pipe "foo")]
    (is (instance? Pipe np))
    (is (= "foo" (.getName np)))))

(deftest test-clojure-filter
  (let [fil (ClojureFilter. (w/fn-spec #'odd?) false)]
    (is (= false (t/invoke-filter fil [1])))
    (is (= true  (t/invoke-filter fil [2])))))

(deftest test-clojure-map-one-field
  (let [m1 (ClojureMap. (w/fields "num") (w/fn-spec #'inc-wrapped) false)
        m2 (ClojureMap. (w/fields "num") (w/fn-spec #'inc) false)]
    (are [m] (= [[2]] (t/invoke-function m [1])) m1 m2)))

(deftest test-clojure-map-multiple-fields
  (let [m (ClojureMap. (w/fields ["num1" "num2"]) (w/fn-spec #'inc-both) false)]
    (is (= [[2 3]] (t/invoke-function m [1 2])))))

(defn iterate-inc-wrapped [num]
  (list [(+ num 1)] [(+ num 2)] [(+ num 3)]))

(defn iterate-inc [num]
  (list (+ num 1) (+ num 2) (+ num 3)))

(deftest test-clojure-mapcat-one-field
  (let [m1 (ClojureMapcat. (w/fields "num") (w/fn-spec #'iterate-inc-wrapped) false)
        m2 (ClojureMapcat. (w/fields "num") (w/fn-spec #'iterate-inc) false)]
    (are [m] (= [[2] [3] [4]] (t/invoke-function m [1])) m1 m2)))

(defn sum
  ([]
     0)
  ([mem v]
     (+ mem v))
  ([mem]
     [mem]))

(deftest test-clojure-aggregator
  (let [a (ClojureAggregator. (w/fields "sum") (w/fn-spec #'sum) false)]
    (is (= [[6]] (t/invoke-aggregator a [[1] [2] [3]])))))
