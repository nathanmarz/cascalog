(ns cascalog.logic.ops
  (:refer-clojure :exclude [count min max comp juxt partial])
  (:use cascalog.api
        [jackknife.def :only (defnk defalias)]
        [jackknife.seq :only (collectify)])
  (:require [cascalog.logic.def :as d]
            [cascalog.logic.fn :as s]
            [cascalog.logic.ops-impl :as impl]
            [cascalog.logic.vars :as v])
  (:import [cascalog.logic.def Prepared ParallelBuffer]))

;; Operation composition functions

(defn negate
  "Accepts a filtering op and returns an new op that acts as the
  negation (or complement) of the original. For example:

  ((negate #'string?) ?string-var) ;; within some query

  Is equivalent to

  ;; within some query
  (string? ?string-var :> ?temp-bool)
  (not ?temp-bool)"
  [op]
  (<- [:<< !invars :> !true?]
      (op :<< !invars :> !curr?)
      (not !curr? :> !true?)))

(defn all
  "Accepts any number of filtering ops and returns a new op that
  checks that every every one of the original filters passes. For
  example:

  ((all #'even? #'positive? #'small?) ?x) ;; within some query

  Is equivalent to:

  ;; within some query
  (even? ?x :> ?temp1)
  (positive? ?x :> ?temp2)
  (small? ?x) :> ?temp3)
  (and ?temp1 ?temp2 ?temp3)"
  [& ops]
  (impl/logical-comp ops #'impl/bool-and))

(defn any
  "Accepts any number of filtering ops and returns a new op that
  checks that at least one of the original filters passes. For
  example:

  ((any #'even? #'positive? #'small?) ?x) ;; within some query

  Is equivalent to:

  ;; within some query
  (even? ?x :> ?temp1)
  (positive? ?x :> ?temp2)
  (small? ?x :> ?temp3)
  (or ?temp1 ?temp2 ?temp3)"
  [& ops]
  (impl/logical-comp ops #'impl/bool-or))

(defn comp
  "Accepts any number of predicate ops and returns an op that is the
  composition of those ops.

  (require '[cascalog.ops :as c])
  ((c/comp #'str #'+) ?x ?y :> ?sum-string) ;; within some query

  Is equivalent to:

  ;; within some query
  (+ ?x ?y :> ?intermediate)
  (str ?intermediate :> ?sum-string)"
  [& ops]
  (let [[invar & more] (v/gen-nullable-vars (inc (clojure.core/count ops)))
        allvars (list* invar (map vector more))]
    (construct [:<< invar :> (last more)]
               (map (fn [o [invars outvars]]
                      [o :<< invars :>> outvars])
                    (reverse ops)
                    (partition 2 1 allvars)))))

(defn juxt
  "Accepts any number of predicate ops and returns an op that is the
  juxtaposition of those ops.

  (require '[cascalog.ops :as c])
  ((c/juxt #'+ #'- #'<) !x !y :> !sum !diff !mult) ;; within some query

  Is equivalent to:

  ;; within some query
  (+ !x !y :> !sum)
  (- !x !y :> !diff)
  (* !x !y :> !mult)"
  [& ops]
  (let [outvars (v/gen-nullable-vars (clojure.core/count ops))]
    (construct
     [:<< "!invars" :>> outvars]
     (map (fn [o v] [o :<< "!invars" :> v])
          ops
          outvars))))

(defn each
  "Accepts an operation and returns a predicate macro that maps `op`
  across any number of input variables. For example:

  ((each #'str) ?x ?y ?z :> ?x-str ?y-str ?z-str) ;; within some query

  Is equivalent to

  ;; within some query
  (str ?x :> ?x-str)
  (str ?y :> ?y-str)
  (str ?z :> ?z-str)"
  [op]
  (predmacro [invars outvars]
             {:pre [(or (zero? (clojure.core/count outvars))
                        (= (clojure.core/count invars)
                           (clojure.core/count outvars)))]}
             (if (empty? outvars)
               (for [i invars]
                 [op i])
               (map (fn [i v] [op i :> v])
                    invars
                    outvars))))

(defn partial
  "Accepts an operation and fewer than normal arguments, and returns a
  new operation that can be called with the remaining unspecified
  args. For example, given this require and defmapop:

  (require '[cascalog.logic.ops :as c])
  (defmapop plus [x y] (+ x y))

  The following two forms are equivalent:

  (let [plus-10 (c/partial plus 10)]
     (<- [?y] (src ?x) (plus-10 ?x :> ?y)))

  (<- [?y] (src ?x) (plus-10 ?x :> ?y))

  With the benefit that `10` doesn't need to be hardcoded into the
  first query."
  [op & args]
  (predmacro
   [invars outvars]
   [[op :<< (concat args invars) :>> outvars]]))

;; Operations to use within queries

(defn re-parse
  "Accepts a regex `pattern` and a string argument `str` and returns
  the groups within `str` that match the supplied `pattern`."
  [pattern]
  (mapfn [str] (re-seq pattern str)))

(defparallelagg count
  :init-var #'impl/one
  :combine-var #'+)

(def sum (each impl/sum-parallel))

(def min (each impl/min-parallel))

(def max (each impl/max-parallel))

(def !count (each impl/!count-parallel))

(defn limit-init [sort-tuple & tuple]
  ;; this is b/c CombinerBase does coerceToSeq on everything and
  ;; applies when combining, since this returns a seq we need an
  ;; extra level of nesting should have a different combiner base
  ;; for buffer combiners
  [[[(vec sort-tuple) (vec tuple)]]])

(defn- mk-limit-comparator [options]
  (s/fn [[^Comparable o1 _] [^Comparable o2 _]]
    (if (:sort options)
      (* (.compareTo o1 o2) (if (boolean (:reverse options)) -1 1))
      0)))

(defn limit-combine [options n]
  (let [compare-fn (mk-limit-comparator options)]
    (s/fn [list1 list2]
      (let [res (concat list1 list2)]
        [(if (> (clojure.core/count res) (* 2 n))
           (take n (sort compare-fn res))
           res)]))))

(defn limit-extract [options n]
  (let [compare-fn (mk-limit-comparator options)]
    (s/fn [alist]
      (let [alist (if (<= (clojure.core/count alist) n)
                    alist
                    (take n (sort compare-fn alist)))]
        (map (clojure.core/partial apply concat) alist)))))

;; Special node. The operation inside of here will be passed the
;; option map for that section of the job.

(defn limit-buffer [n]
  (s/fn [tuples]
    (take n tuples)))

(defn limit-rank-buffer [n]
  (s/fn [tuples]
    (take n (map #(conj (vec %1) %2) tuples (iterate inc 1)))))

(defn- limit-maker [n buffer-fn]
  (d/Prepared.
   (fn [options]
     (d/ParallelBuffer. #'limit-init
                         (limit-combine options n)
                         (limit-extract options n)
                         (fn [infields outfields]
                           (clojure.core/count infields))
                         buffer-fn))))

(defn limit [n]
  (limit-maker n (limit-buffer n)))

(defn limit-rank [n]
  (limit-maker n (limit-rank-buffer n)))

(def avg
  "Predicate operation that produces the average value of the
  supplied input variable. For example:

  (let [src [[1] [2]]]
    (<- [?avg]
        (src ?x)
        (avg ?x :> ?avg)))
  ;;=> ([1.5])"
  (<- [!v :> !avg]
      (count !c)
      (sum !v :> !s)
      (div !s !c :> !avg)))

(def distinct-count
  "Predicate operation that produces a count of all distinct
  values of the supplied input variable. For example:

  (let [src [[1] [2] [2]]]
  (<- [?count]
      (src ?x)
      (distinct-count ?x :> ?count)))
  ;;=> ([2])"
  (<- [:<< !invars :> !c]
      (:sort :<< !invars)
      (impl/distinct-count-agg :<< !invars :> !c)))

(defn fixed-sample-agg [amt]
  (<- [:<< ?invars :>> ?outvars]
      ((cascalog.ops.RandLong.) :<< ?invars :> ?rand)
      (:sort ?rand)
      ((limit amt) :<< ?invars :>> ?outvars)))

(defn fixed-sample
  "Returns a subquery getting a random sample of n elements from the generator"
  [gen n]
  (let [num-fields (num-out-fields gen)
        in-vars  (v/gen-nullable-vars num-fields)
        out-vars (v/gen-nullable-vars num-fields)]
    (<- out-vars
        (gen :>> in-vars)
        ((fixed-sample-agg n) :<< in-vars :>> out-vars))))

;; Common patterns

(defnk first-n
  "Accepts a generator and a number `n` and returns a subquery that
   produces the first n elements from the supplied generator. Two
   boolean keyword arguments are supported:

  :sort -- accepts a vector of variables on which to sort. Defaults to
           nil (unsorted).
  :reverse -- If true, sorts items in reverse order. (false by default).

  For example:

  (def src [[1] [3] [2]]) ;; produces 3 tuples

  ;; produces ([1 2] [3 4] [2 3]) when executed
  (def query (<- [?x ?y] (src ?x) (inc ?x :> ?y)))

  ;; produces ([3 4]) when executed
  (first-n query 1 :sort [\"?x\"] :reverse true)"
  [gen n :sort nil :reverse false]
  (let [num-fields (num-out-fields gen)
        in-vars  (v/gen-nullable-vars num-fields)
        out-vars (v/gen-nullable-vars num-fields)
        sort-set (if sort (-> sort collectify set) #{})
        sort-vars (if sort
                    (mapcat (fn [f v] (if (sort-set f) [v]))
                            (get-out-fields gen)
                            in-vars))]
    (<- out-vars
        (gen :>> in-vars)
        (:sort :<< sort-vars)
        (:reverse reverse)
        ((limit n) :<< in-vars :>> out-vars))))
