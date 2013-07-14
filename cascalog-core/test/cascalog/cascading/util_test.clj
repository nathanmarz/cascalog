(ns cascalog.cascading.util-test
  (:use midje.sweet
        cascalog.cascading.util)
  (:import [cascading.tuple Fields]
           [cascading.pipe Pipe]
           [cascalog.aggregator ClojureAggregator]
           [cascalog ClojureFilter ClojureMap ClojureMapcat]))

(defn plus-one [in]
  [(+ in 1)])

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
  (let [f1 (fields "foo")
        f2 (fields ["foo" "bar"])]
    (facts "Single fields should resolve properly."
      f1       => (is-type Fields)
      (seq f1) => ["foo"])

    (facts "Double fields should resolve properly."
      f2       => (is-type Fields)
      (seq f2) => ["foo" "bar"])))

(tabular
 (fact "Pipes without names use UUID."
   ?pipe => (is-type Pipe)
   (.getName ?pipe) => ?check)
 ?pipe         ?check
 (pipe)        #(= 36 (count %))
 (pipe "name") "name")

(fact "Clojure Filter test."
  (let [fil (ClojureFilter. odd?)]
    (invoke-filter fil [1]) => false
    (invoke-filter fil [2]) => true))

(tabular
 (fact "ClojureMap test, single field."
   (invoke-function ?clj-map [1]) => [[2]])
 ?clj-map
 (ClojureMap. (fields "num")
              inc-wrapped)
 (ClojureMap. (fields "num")
              inc))

(facts "ClojureMap test, multiple fields."
  (let [m (ClojureMap. (fields ["num1" "num2"]) inc-both)]
    (invoke-function m [1 2]) => [[2 3]]))

(defn iterate-inc-wrapped [num]
  (list [(+ num 1)]
        [(+ num 2)]
        [(+ num 3)]))

(defn iterate-inc [num]
  (list (+ num 1)
        (+ num 2)
        (+ num 3)))

(tabular
 (fact
   "ClojureMapCat test, single field. Wrapped vs non-wrapped should
      have the same result, when a single field is involved."
   (invoke-function ?clj-mapcat [1]) => [[2] [3] [4]])
 ?clj-mapcat
 (ClojureMapcat. (fields "num") iterate-inc-wrapped)
 (ClojureMapcat. (fields "num") iterate-inc))

(defn sum
  ([] 0)
  ([mem v] (+ mem v))
  ([mem] [mem]))

(fact "ClojureAggregator test."
  (let [a (ClojureAggregator. (fields "sum") sum)]
    (invoke-aggregator a [[1] [2] [3]]) => [[6]]))
