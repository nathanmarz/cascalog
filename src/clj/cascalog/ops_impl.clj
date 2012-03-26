;;    Copyright 2010 Nathan Marz
;; 
;;    This program is free software: you can redistribute it and/or modify
;;    it under the terms of the GNU General Public License as published by
;;    the Free Software Foundation, either version 3 of the License, or
;;    (at your option) any later version.
;; 
;;    This program is distributed in the hope that it will be useful,
;;    but WITHOUT ANY WARRANTY; without even the implied warranty of
;;    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;;    GNU General Public License for more details.
;; 
;;    You should have received a copy of the GNU General Public License
;;    along with this program.  If not, see <http://www.gnu.org/licenses/>.

(ns cascalog.ops-impl
  (:use cascalog.api)
  (:require [cascalog.vars :as v]
            [cascalog.workflow :as w]))

(defn one [] 1)

(defn identity-tuple [& tuple] tuple)

(defn existence-int [v] (if v 1 0))

(defparallelagg sum-parallel
  :init-var #'identity
  :combine-var #'+)

(defparallelagg min-parallel
  :init-var #'identity
  :combine-var #'min)

(defparallelagg max-parallel
  :init-var #'identity
  :combine-var #'max)

(defparallelagg !count-parallel
  :init-var #'existence-int
  :combine-var #'+)

(defn limit-init [options limit]
  (fn [sort-tuple & tuple]
    ;; this is b/c CombinerBase does coerceToSeq on everything and applies when combining,
    ;; since this returns a seq we need an extra level of nesting
    ;; should have a different combiner base for buffer combiners
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
    (take limit (map #(conj (vec %1) %2) tuples (iterate inc 1)))))

(def RANDOM (java.util.Random.))

(defn sample-init
  "emit a single tuple at a time"
  [_ reservoir-size]
  (fn [sort-tuple & tuple] 
    [[1 [[(vec sort-tuple) (vec tuple)]]]]))

(defn- maybe-insert
  "determine whether or not to replace a tuple in `reservoir` with `item`."
  [reservoir item reservoir-size seen]
  (let [r (.nextInt RANDOM seen)]
    (if (< r reservoir-size)
      (assoc reservoir r item)
      reservoir)))

(defn sample-combine
  "`list1` is the output of a previous `sample-combine`, or the output of
  `sample-init` if `sample-combine` hasn't run yet.

  `list2` is the output of `sample-init` and is assume to always be a single tuple.

  if `seen` is < `reservoir-size`, add `list2` to `list1`, which represent the reservoir.
  otherwise call `maybe-insert` on `list2`."
  [_ reservoir-size]
  (fn [list1 list2]
    (let [seen (+ (first list1) (first list2))
          list1 (second list1)
          list2 (second list2)
          res (conj list1 (first list2))]
      [[seen (if (> (count res) reservoir-size)
         (maybe-insert list1 (first list2) reservoir-size seen)
         res)]]
      )))

(defn sample-extract
  "after all tuples have passed through `sample-combine`, this function is called.

  NB. this function must take only a single argument or an exception is thrown."
  [_ reservoir-size]
  (fn [alist]
    (let [seen (first alist)
          alist (second alist)
          values (map (partial apply into) alist)]
      (map #(conj % seen) values))))

(defn sample-buffer
  "the results of all the distributed `sample-extract` operations are combined
  into a single collection of tuples and a weighted reservoir sampling algorithm
  as described at http://gregable.com/2007/10/reservoir-sampling.html is performed."
  [_ reservoir-size]
  (fn [tuples]
    (let [total-seen (reduce + (distinct (map last tuples)))
          mk-key (fn [size seen] (Math/pow (.nextDouble RANDOM) (/ 1 (/ (double seen) total-seen))))
          generate-keys (fn [x] (conj (vec (butlast x)) (mk-key reservoir-size (last x))))
          samples (map #(generate-keys %) tuples)]
      (map butlast (take reservoir-size (sort-by last > samples))))))

(w/defaggregateop distinct-count-agg
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

