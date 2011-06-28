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

(defmapop [ident-stateful [y]]
  "Identity operation."
  {:stateful true
   :great-meta "yes!"}
  ([] 0)
  ([state x] (+ x y))
  ([state] nil))

(deftest parse-defop-args-test
  (let [src [[1] [2]]]
    (test?<- [[2] [3]] [?y] (src ?x) (ident-stateful [1] ?x :> ?y))
    (doseq [func [ident ident-doc ident-meta ident-both]]
      (test?<- [[1] [2]] [?y] (src ?x) (func ?x :> ?y))
      )))