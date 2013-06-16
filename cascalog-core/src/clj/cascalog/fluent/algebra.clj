(ns cascalog.fluent.algebra)

(defprotocol Semigroup
  "First step toward an abstract algebra library."
  (plus [l r]))

(extend-protocol Semigroup
  nil
  (plus [l r] r)

  clojure.lang.IPersistentVector
  (plus [l r] (concat l r))

  clojure.lang.IPersistentList
  (plus [l r] (concat l r))

  clojure.lang.IPersistentMap
  (plus [l r]
    (merge-with plus l r)))

(defn sum [items]
  (reduce plus items))
