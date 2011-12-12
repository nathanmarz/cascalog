(ns cascalog.defops-test
  (:use cascalog.api
        cascalog.testing
        clojure.test))

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

(defmapop ident-stateful
  "Identity operation."
  {:params [y]
   :stateful true
   :great-meta "yes!"}
  ([] 3)
  ([state x] (+ x y state))
  ([state] nil))

(def ident-stateful-single-arg (ident-stateful 1))
(def ident-stateful-vec-args (ident-stateful [1]))

(deftest parse-defop-args-test
  (let [src [[1] [2]]]
    (test?<- [[5] [6]] [?y] (src ?x) ((ident-stateful [1]) ?x :> ?y))
    (test?<- [[5] [6]] [?y] (src ?x) ((ident-stateful 1) ?x :> ?y))
    (test?<- [[5] [6]] [?y] (src ?x) (ident-stateful-single-arg ?x :> ?y))
    (test?<- [[5] [6]] [?y] (src ?x) (ident-stateful-vec-args ?x :> ?y))
    (doseq [func [ident ident-doc ident-meta ident-both]]
      (test?<- [[1] [2]] [?y] (src ?x) (func ?x :> ?y)))))

(deftest get-metadata-test
  (is (= "yes!" (:great-meta (meta ident-stateful))))
  (is (= "yes!" (:great-meta (meta #'ident-stateful))))
  (is (= "Identity operation." (:doc (meta ident-doc))))
  (is (= "Identity operation." (:doc (meta #'ident-doc))))
  (is (= nil (:doc (meta #'ident-meta)))))

(defn five->two [a b c d e]
  [(+ a b c) (+ d e)])

(defn four->one [a b c d]
  (+ a b c d))

(defparallelagg multi-combine
  :init-var #'five->two
  :combine-var #'four->one)

(deftest multiple-aggregator-args-test
  (let [src [[1 2 3 4 5] [5 6 7 8 9]]]
    "init-var takes n args, outputs x. combine-var takes 2*x args,
     outputs x."
    (test?<- [[50]]
             [?sum]
             (src ?a ?b ?c ?d ?e)
             (multi-combine ?a ?b ?c ?d ?e :> ?sum))))
