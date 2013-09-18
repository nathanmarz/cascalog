(ns cascalog.logic.ops-impl
  (:use cascalog.api)
  (:require [cascalog.logic.vars :as v]))

(defn one [] 1)

(defn identity-tuple [& tuple] tuple)

(defn existence-int [v] (if v 1 0))

(defn nil-safe-combiner 
  "Takes a function f of two variables, and returns a function
   of two variables that calls f on its arguments only if both
   are non-nil. If one argument is nil, it prefers the other argument,
   and if both are nil it returns nil."
  [f]
  (fn [x y]
    (cond 
     (and (nil? x) (nil? y)) nil
     (nil? x) y
     (nil? y) x
     :else (f x y))))

(defn smaller [x y]
  (if (neg? (compare x y)) x y))

(defn larger [x y]
  (if (pos? (compare x y)) x y))

(def comparable-min-impl (nil-safe-combiner smaller))

(def comparable-max-impl (nil-safe-combiner larger))

(defn distinct-count-impl [prev cnt tuple]
  (if (or (every? nil? tuple) (= tuple prev))
    cnt
    (inc cnt)))

(defparallelagg sum-parallel
  :init-var #'identity
  :combine-var #'+)

(defparallelagg min-parallel
  :init-var #'identity
  :combine-var #'min)

(defparallelagg comparable-min-parallel 
  :init-var #'identity
  :combine-var #'comparable-min-impl)

(defparallelagg max-parallel
  :init-var #'identity
  :combine-var #'max)

(defparallelagg comparable-max-parallel 
  :init-var #'identity
  :combine-var #'comparable-max-impl)

(defparallelagg !count-parallel
  :init-var #'existence-int
  :combine-var #'+)

(defn limit-init [options _]
  (fn [sort-tuple & tuple]
    ;; this is b/c CombinerBase does coerceToSeq on everything and
    ;; applies when combining, since this returns a seq we need an
    ;; extra level of nesting should have a different combiner base
    ;; for buffer combiners
    [[[(vec sort-tuple) (vec tuple)]]]))

(defn- mk-limit-comparator [options]
  (fn [[^Comparable o1 _] [^Comparable o2 _]]
    (if (:sort options)
      (* (.compareTo o1 o2) (if (boolean (:reverse options)) -1 1))
      0)))

(defn limit-combine [options limit]
  (let [compare-fn (mk-limit-comparator options)]
    (fn [list1 list2]
      (let [res (concat list1 list2)]
        ;; see note in limit-init
        [(if (> (count res) (* 2 limit))
           (take limit (sort compare-fn res))
           res)]))))

(defn limit-extract [options limit]
  (let [compare-fn (mk-limit-comparator options)]
    (fn [alist]
      (let [alist (if (<= (count alist) limit)
                    alist
                    (take limit (sort compare-fn alist)))]
        (map (partial apply concat) alist)))))

(defn limit-buffer [_ limit]
  (fn [tuples]
    (take limit tuples)))

(defn limit-rank-buffer [_ limit]
  (fn [tuples]
    (take limit (map (fn [x y] (conj (vec x) y))
                     tuples
                     (iterate inc 1)))))

(defaggregatefn distinct-count-agg
  ([] [nil 0])
  ([[prev cnt] & tuple]
     [tuple (if (= tuple prev) cnt (inc cnt))])
  ([state] [(second state)]))

(defn bool-or [& vars]
  (boolean (some identity vars)))

(defn bool-and [& vars]
  (every? identity vars))

(defn logical-comp [ops logic-fn-var]
  (let [outvars (v/gen-nullable-vars (clojure.core/count ops))]
    (construct
     [:<< "!invars" :> "!true?"]
     (conj
      (map (fn [o v] [o :<< "!invars" :> v]) ops outvars)
      [logic-fn-var :<< outvars :> "!true?"]))))
