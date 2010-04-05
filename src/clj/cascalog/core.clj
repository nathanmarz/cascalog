(ns cascalog.core
  (:use [cascalog vars util])
  (:require [cascalog [workflow :as w] [predicate :as p]])
  (:import [cascading.tap Tap])
  (:import [cascading.tuple Fields]))

;; TODO:
;; 
;; 4. Enforce !! rules -> only allowed in generators or output of operations, ungrounds whatever it's in

;; TODO: make it possible to create ungrounded rules that take in input vars (for composition)
;; i.e. (<- [?a ?b :> ?c] (func1 ?a :> ?c) (func2 ?b :> ?c))
;; (<- [?p] (data ?p ?a ?b) (func1 ?a :> ?c1) (func2 ?b :> ?c2) (= ?c1 ?c2))
;; (<- [?p] (age ?p ?a) (friend ?p1 ?p2) (= ?p ?p1))
;; (<- [?num] (nums ?num ?num) (source2 ?num))
;; (<- [?num] (nums ?num1 ?num2) (source2 ?num3) (= ?num1 ?num2 ?num3))
;; -> do filter equalities as you can, then do joins when they're valid
;; (<- [?p1 ?p2] (age ?p1 ?a) (age ?p2 ?a) (friend ?p1 ?p2))
;; (<- [?p1 ?p2] (age ?p1 ?a) (age ?p2 ?a1) (friend ?p1 ?p2) (= ?a ?a1))
;; TODO: what's the example of needing a maximal join?
;; second-degree-age (<- [?a ?p :> ?p2] (friend ?p ?p1) (friend ?p1 ?p2) (age ?p2 :> ?a))
;; TODO: variable renaming needs to create extra equality relationships (everything is uniqued)


; (let [many-friends (<- [?p] (friend ?p _) (count ?c) (> ?c 100))
;       connected-influencers (<- [?p1 ?p2] (many-friends ?p1) (many-friends ?p2) (friend ?p1 ?p2))]
;       ;; should we just replicate many-friends twice, or do a self-join?
;       many-friends -> ?p --> ?p1  -> friend(?p1 ?p_)-\
;                          \                            -->join
;                           -> ?p2  -------------------/
; 
;       many-friends -> ?p1  -> friend(?p1 ?p_)-\
;                                                -->join
;       many-friends -> ?p2 --------------------/
;   )

;; assembly approach requires no self-joins or pipe renaming... should just work
;;    -> inefficient when an aggregation and THEN a self-join (only way to fix this is to do 2 flows)
;;    -> if necessary, a user can manually do 2 queries
;; pipe approach requires pipe renaming and sometimes won't work (joining pipes that originate from same source 
;;   and haven't been through any reducing)
;; the bad case is aggregating off a source, and then doing 2 branches without reducing and self-joining
;; for most queries doesn't matter... but connected-influencers would be bad -> would cascaing duplicate
;; the computation anyway?
;; ideally just thread one pipe from each source & branch and merge
;; need to deal with self-join problem with renaming
;; what do do about self-join with no reduces in branches?, i.e. (age ?p ?a) (age ?p2 ?a)
;;                        (friend ?p1 ?p2) (friend ?p2 ?p3)
;; every rule has a reduce (at least a distinct). just need to detect self-joins within a rule

;; (<- age(?p :> ?a1) (> ?a2 5) (= ?a1 ?a2))
;; (<- part1 [?p] ...)
;; (<- part2 [?p] (part1 ?p) (somefilter ?p))
;; (?- [part1 ... part2...]) ... won't even be connected w/ assembly approach- what will happen - it works
;;  check flow test
;; don't even support this, just have:
;; (?- tap assem)
;; (?<- tap vars preds) -> 
;;    (?- tap (<- vars preds))
;; what if i just uniquify and not keep an equality index?
;; age(?p :> ?a) (> ?a 5)
(defn build-rule [out-vars raw-predicates]
  (let [[out-vars vmap]       (uniquify-vars out-vars {})
        update-fn             (fn [[preds vmap] [op opvar vars]]
                                (let [[newvars vmap] (uniquify-vars vars vmap)]
                                  [(conj preds [op opvar newvars]) vmap] ))
        [raw-predicates vmap] (reduce update-fn [[] vmap] raw-predicates)
        predicates            (map (partial apply p/build-predicate) raw-predicates)]
        ;; make a graph

        ;; now, need to make a generator predicate (defstruct generator-predicate :type :sources :assembly :outfields)
        ;; start with existing generators, stack functions, filters, and add joins or equality filters according to 
        ;;  the var index
        ;; make a graph, final being a projection operation to out-vars
        ;; can add additional analysis to the graph (for example, dead variable analysis and projections)
        ;; should i make a stateful graph? how to do functional graphs?
        ;; project output tuple according to 
        ;; want to push functions up and push filters down
        ;; 1. add functions and filters to fixed point
        ;; 2. 
        ;; 1. start with all generators as separate nodes 
        ;; 2. Add filters
        ;; 3. search for a join, applying functions as necessary for a maximal join
        (println vmap)
        predicates
    ))

(defn- mk-raw-predicate [pred]
  (let [[op-sym & vars] pred
        str-vars (vars2str vars)]
    [op-sym (try-resolve op-sym) str-vars]))

(defmacro <-
  "Constructs a rule from a list of predicates"
  [outvars & predicates]
  (let [predicate-builders (vec (map mk-raw-predicate predicates))
        outvars-str (vars2str outvars)]
        `(cascalog.core/build-rule ~outvars-str ~predicate-builders)))

