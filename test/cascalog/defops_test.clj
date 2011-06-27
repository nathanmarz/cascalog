(ns cascalog.defops-test
  (:use cascalog.api
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
    (is (= [[2] [3]] (??<- [?y] (src ?x) (ident-stateful [1] ?x :> ?y))))
    (are [res func] (= res (??<- [?y] (src ?x) (func ?x :> ?y)))
         src ident
         src ident-doc
         src ident-meta
         src ident-both)))
