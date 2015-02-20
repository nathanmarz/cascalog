(ns cascalog.logic.algebra)

(defprotocol Semigroup
  "First step toward an abstract algebra library."
  (plus [l r]))

(extend-protocol Semigroup
  nil
  (plus [l r] r)

  String
  (plus [l r]
    (str l r))

  clojure.lang.IPersistentVector
  (plus [l r] (concat l r))

  clojure.lang.IPersistentList
  (plus [l r] (concat l r))

  clojure.lang.IPersistentMap
  (plus [l r]
    (merge-with plus l r))

  clojure.lang.LazySeq
  (plus [l r]
    (lazy-cat l r))

  java.lang.Integer
  (plus [l r] (+ l r))

  java.lang.Double
  (plus [l r] (+ l r))

  java.lang.Float
  (plus [l r] (+ l r))

  java.lang.Long
  (plus [l r] (+ l r))

  clojure.lang.Ratio
  (plus [l r] (+ l r)))

(defn sum [items]
  (reduce plus items))
