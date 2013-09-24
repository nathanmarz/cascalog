(ns cascalog.logic.defops-test
  (:use cascalog.api
        clojure.test
        [midje sweet cascalog])
  (:require [cascalog.logic.ops]))

(defmapop ident [x] x)

(defmapop ident-doc
  "Identity operation."
  [x] x)

(defmapop ident-meta
  {:great-meta "yes!"}
  [x] x)

(defmapop ident-both
  "Identity operation."
  {:great-meta "yes!"}
  [x] x)

(defn ident-stateful
  {:great-meta "yes!"}
  [y]
  "Identity operation."
  (let [state 3]
    (mapfn [x] (+ x y state))))

(deftest defops-arg-parsing-test
  (let [src      [[1] [2]]
        mk-query (fn [afn]
                   (<- [?y] (src ?x) (afn ?x :> ?y)))]

    "This query should add 3 plus the param to each input var from
    src."
    (fact (<- [?y]
              (src ?x)
              ((ident-stateful 1) ?x :> ?y))
      => (produces [[5] [6]]))
    (tabular
     (fact
       "Each function will be applied to `mk-query` in turn; all of
       these functions act as identity transformations, so each query
       should produce the original source without modification."
       (mk-query ?func) => (produces src))
     ?func
     ident
     ident-doc
     ident-meta
     ident-both)))

(deftest metadata-test
  (facts "Metadata testing."
    "var should have custom metadata."
    (meta #'ident-stateful) => (contains {:great-meta "yes!"})

    "var should have a docstring."
    (meta #'ident-doc) => (contains {:doc "Identity operation."})

    "ident-meta shouldn't have a docstring in its metadata."
    (meta #'ident-meta) =not=> (contains {:doc anything})))

(defn five->two [a b c d e]
  [(+ a b c) (+ d e)])

(defn four->one [a b c d]
  (+ a b c d))

(defparallelagg multi-combine
  :init-var #'five->two
  :combine-var #'four->one)

(deftest agg-test
  (fact "Test of aggregators with multiple arguments."
    (let [src [[1 2 3 4 5] [5 6 7 8 9]]]
      "init-var takes n args, outputs x. combine-var takes 2*x args,
     outputs x."
      (fact?<- [[50]]
               [?sum]
               (src ?a ?b ?c ?d ?e)
               (multi-combine ?a ?b ?c ?d ?e :> ?sum)))))

(deftest !sum-test
  (let [src [["a" 1]
             ["a" 2]
             ["b" 5]
             ["b" nil]
             ["c" nil]]]
    (fact
     (<- [?id !total]
         (src ?id !x)
         (cascalog.logic.ops/!sum !x :> !total))
     => (produces [["a" 3]
                   ["b" 5]
                   ["c" nil]]))))

(deftest !avg-test
  (let [src [["a" 1]
             ["a" 2]
             ["b" 5]
             ["b" nil]
             ["c" nil]]]
    (fact
     (<- [?id !avg]
         (src ?id !x)
         (cascalog.logic.ops/!avg !x :> !avg))
     => (produces [["a" 1.5]
                   ["b" 5.0]
                   ["c" nil]]))))

(deftest !min-test
  (let [src [["a" 1]
             ["a" 2]
             ["b" 5]
             ["b" nil]
             ["c" nil]]]
    (fact
     (<- [?id !min]
         (src ?id !x)
         (cascalog.logic.ops/!min !x :> !min))
     => (produces [["a" 1]
                   ["b" 5]
                   ["c" nil]]))))

(deftest !max-test
  (let [src [["a" 1]
             ["a" 2]
             ["b" 5]
             ["b" nil]
             ["c" nil]]]
    (fact
     (<- [?id !max]
         (src ?id !x)
         (cascalog.logic.ops/!max !x :> !max))
     => (produces [["a" 2]
                   ["b" 5]
                   ["c" nil]]))))

(deftest !distinct-count-test
  (let [src [["a" 1]
             ["a" 2]
             ["b" 5]
             ["b" nil]
             ["c" nil]]]
    (fact
     (<- [?id !distinct-count]
         (src ?id !x)
         (cascalog.logic.ops/!distinct-count !x :> !distinct-count))
     => (produces [["a" 2]
                   ["b" 1]
                   ["c" 0]]))))

