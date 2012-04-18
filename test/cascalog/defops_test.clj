(ns cascalog.defops-test
  (:use cascalog.api
        clojure.test
        [midje sweet cascalog]))

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
  "Identity operation."
  {:great-meta "yes!"}
  [y]
  (prepmapop [process opcall]
    (let [state 3]
      (fn [x]
        (+ x y state)
        ))))

(deftest defops-arg-parsing-test
  (let [src      [[1] [2]]
        mk-query (fn [afn]
                   (<- [?y] (src ?x) (afn ?x :> ?y)))]

    "This query should add 3 plus the param to each input var from
    src."
    (fact?<- [[5] [6]]
             [?y]
             (src ?x)
             ((ident-stateful 1) ?x :> ?y))
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

(facts "Metadata testing."
  "Var should contain custom metadata."
  (meta #'ident-stateful) => (contains {:great-meta "yes!"})

  "Var should contain docstring."
  (meta #'ident-doc) => (contains {:doc "Identity operation."})

  "ident-meta shouldn't have a docstring in its metadata."
  (meta #'ident-meta) =not=> (contains {:doc anything}))


(defn five->two [a b c d e]
  [(+ a b c) (+ d e)])

(defn four->two [a b c d]
  [(+ a c) (+ b d)])

(defparallelagg multi-combine
  :init five->two
  :combine four->two)

(deftest agg-test
  (fact "Test of aggregators with multiple arguments."
   (let [src [[1 2 3 4 5] [5 6 7 8 9]]]
     "init-var takes n args, outputs x. combine-var takes 2*x args,
     outputs x."
     (fact?<- [[24 26]]
              [?sum1 ?sum2]
              (src ?a ?b ?c ?d ?e)
              (multi-combine ?a ?b ?c ?d ?e :> ?sum1 ?sum2)))))
