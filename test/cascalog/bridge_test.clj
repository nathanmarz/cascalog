(ns cascalog.bridge-test
  (:use midje.sweet)
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

(defn is-type
  "Accepts a class and returns a checker that tests whether or not its
  input is an instance of the supplied class."
  [^Class expected]
  (chatty-checker
   [actual]
   (instance? expected actual)))


(facts "Fields tests."
  (let [f1 (w/fields "foo")
        f2 (w/fields ["foo" "bar"])]
    (facts "Single fields should resolve properly."
      f1       => #(instance? Fields %)
      (seq f1) => ["foo"])

    (facts "Double fields should resolve properly."
      f2       => #(instance? Fields %)
      (seq f2) => ["foo" "bar"])))

(tabular 
 (fact "Pipe testing."
   ?pipe => #(instance? Pipe %)
   (.getName ?pipe) => ?check)
 ?pipe           ?check
 (w/pipe)        #(= 36 (.length %))
 (w/pipe "name") "name")

(fact "Clojure Filter test."
  (let [fil (ClojureFilter. odd?)]
    (t/invoke-filter fil [1]) => false
    (t/invoke-filter fil [2]) => true))

(tabular
 (fact "ClojureMap test, single field."
   (t/invoke-function ?clj-map [1]) => [[2]])
 ?clj-map
 (ClojureMap. (w/fields "num")
              inc-wrapped)
 (ClojureMap. (w/fields "num")
              inc))

(facts "ClojureMap test, multiple fields."
  (let [m (ClojureMap. (w/fields ["num1" "num2"])
                       inc-both)]
    (t/invoke-function m [1 2]) => [[2 3]]))

(defn iterate-inc-wrapped [num]
  (list [(+ num 1)]
        [(+ num 2)]
        [(+ num 3)]))

(defn iterate-inc [num]
  (list (+ num 1)
        (+ num 2)
        (+ num 3)))

(tabular
 (fact "ClojureMapCat test, single field."
   (t/invoke-function ?clj-mapcat [1]) => [[2] [3] [4]])
 ?clj-mapcat
 (ClojureMapcat. (w/fields "num")
                 iterate-inc-wrapped)
 (ClojureMapcat. (w/fields "num")
                 iterate-inc))

(defn sum
  ([] 0)
  ([mem v] (+ mem v))
  ([mem] [mem]))

(fact "ClojureAggregator test."
  (let [a (ClojureAggregator. (w/fields "sum")
                              sum)]
    (t/invoke-aggregator a [[1] [2] [3]]) => [[6]]))
