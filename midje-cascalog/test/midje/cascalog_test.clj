(ns midje.cascalog-test
  (:use midje.sweet
        clojure.test
        cascalog.api
        midje.cascalog
        [midje.cascalog.impl :only [execute]]
        [clojure.math.combinatorics :only [permutations]])
  (:require [cascalog.ops :as c]))

;; ## Testing Battery

(defn whoop [x] [[x]])
(defn bang [x y] [[x y]])

(defn my-query [x y z]
  (let [foo (whoop x)
        bar (bang y z)]
    (<- [?a ?b]
        (foo ?a)
        (bar ?a ?b))))

(defn a-query [x] (<- [?a] (x ?a)))

(deftest against-background-test
  (fact (whoop :a) => 10
    (provided (whoop :a) => 10)
    (against-background (whoop :a) => 2))
  (against-background [(whoop :a) => 10]
                      (fact (whoop :a) => 10)))

;; Similar to clojure.test's "are".
(deftest tabular?-test
  (tabular?-
    (fact?- ?res (apply ?func ?args))
    ?res    ?func    ?args
    [[3 5]] my-query [3 3 5]
    [[1]]   a-query  [[1]]))

(deftest fact?<-test
  "Use fact?<- to tests and define a function at the same time."
  (fact?<- [[10]]
           [?a]
           ((whoop :a) ?a)
           (provided (whoop :a) => [[10]])))

(deftest background-fact<-test
  (let [result-seq [[3 5]]]
    "Showing that we can draw from the background."
    (fact?- result-seq (my-query .a. .a. .b.)
            [[3 10]]   (my-query .a. .a. .c.)
            (against-background
              (whoop .a.) => [[3]]
              (bang .a. .b.) => [[3 5]]
              (bang .a. .c.) => [[3 10]])))
  (fact?- "the first query pulls from the stuff defined in
          against-background down below."

          [[12 15]] (my-query .a. .a. .b.)

          "The provided block applies to this query..."
          [[100 2]] (my-query .a. .a. .b.)
          (provided (whoop .a.) => [[100]]
                    (bang .a. .b.) => [[100 2]])

          "And, again, drawing from the background."
          [] (my-query .a. .d. .e.)

          (against-background
            (whoop .a.) => [[12]]
            (bang .a. .b.) => [[12 15]]
            (bang .d. .e.) => [[10 15]]))) 

(deftest background-fact?<-test
  (fact?<- "the provided and background clauses work at the end of
           fact?<- as well."
           [[10]]
           [?a]
           ((whoop) ?a)
           (provided (whoop) => [[10]])) 
  (fact?<- "the provided and background clauses work at the end of
           fact?<- as well."
           [[10 11]]
           [?a ?b]
           ((whoop) ?a ?b)
           ((bang) ?b)
           (against-background
             (whoop) => [[10 11] [12 13]]
             (bang)  => [[11]]))) 

;; ## Standard Checker Tests

(deftest produces-test
  (fact
    "The produces checker allows for more midje-like syntax in cascalog
    tests."
    (a-query [[10]]) => (produces [[10]])
    (<- [?a ?b]
        ((whoop) ?a ?b)
        ((bang) ?b :> true)) => (produces [[10 11]])
    (against-background
      (whoop) => [[10 11] [12 13]]
      (bang)  => [[11]])) 
  (let [some-seq [[10]]]
    (fact
      "use `produces` to check that the supplied query, when executed,
      produces exactly the supplied set of tuples -- no more, no less --
      in any order."
      (<- [?a] ((whoop :a) ?a)) => (produces some-seq)
      (provided (whoop :a) => [[10]])))) 

;; The following facts demonstrate the power of midje-cascalog's
;; chatty checkers. Note that each of the forms (`produces`,
;; `produces-some`, `produces-prefix` and `produces-suffix`) can be
;; provided with a log-level keyword as their first argument after the
;; sequence of result tuples.
(deftest chatty-checkers-test
  (let [src   [[1 2] [1 3]
               [3 4] [3 6]
               [5 2] [5 9]]
        query (<- [?x ?sum]
                  (src ?x ?y)
                  (:sort ?x)
                  (c/sum ?y :> ?sum))]
    (facts
      "Executing the query produces proper sums in either order."
      query => (produces [[3 10] [1 5] [5 11]])
      query => (produces [[1 5] [3 10] [5 11]])

      "the `:in-order` keyword makes ordering important, helpful in
      cases where output is sorted."
      query =not=> (produces [[3 10] [5 11] [1 5]] :in-order)
      query => (produces [[1 5] [3 10] [5 11]] :in-order)

      "`produces-some` allows for checking against a subset of tuples"
      query => (produces-some [[5 11] [1 5]])

      "`:in-order` makes ordering important, but gaps are all right."
      query =not=> (produces-some [[5 11] [1 5]] :in-order)
      query => (produces-some [[1 5] [5 11]] :in-order)

      "Adding `:no-gaps` causes gapped tuples to fail."
      query =not=> (produces-some [[1 5] [5 11]] :in-order :no-gaps)
      query => (produces-some [[1 5] [3 10]] :in-order :no-gaps)

      "`produce-prefix` mimics the `has-prefix` collection checker."
      query => (produces-prefix [[1 5]])
      query => (produces-prefix [[1 5] [3 10]])

      "`produce-suffix` mimics the `has-suffix` collection checker."
      query => (produces-suffix [[5 11]])))) 

(defn- mk-query [src]
  (<- [?a] (src ?a)))

;; This syntax makes it possible to wrap tests in an external
;; `against-background` form, like so:

(deftest external-against-background-test
  (against-background
    [(whoop :a) => [[1] [2] [3]]]
    (fact "the background above applies to each fact."
      (mk-query (whoop :a)) => (produces [[1] [2] [3]])

      "Internal calls to provide will override the background."
      (mk-query (whoop :a)) => (produces [["STRING!"]])
      (provided
        (whoop :a) => [["STRING!"]])))) 

(deftest log-level-test
  (doseq [?options (permutations [:in-order :no-gaps :info ])]
    (fact "log-level option is used when executing query -
          regardless of location in options order"
      ((apply produces-some ..query.. ?options) ..query..) => true
      (provided
        (execute ..query.. :log-level :info) => ..query..)))) 
