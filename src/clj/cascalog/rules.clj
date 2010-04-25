 ;    Copyright 2010 Nathan Marz
 ; 
 ;    This program is free software: you can redistribute it and/or modify
 ;    it under the terms of the GNU General Public License as published by
 ;    the Free Software Foundation, either version 3 of the License, or
 ;    (at your option) any later version.
 ; 
 ;    This program is distributed in the hope that it will be useful,
 ;    but WITHOUT ANY WARRANTY; without even the implied warranty of
 ;    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 ;    GNU General Public License for more details.
 ; 
 ;    You should have received a copy of the GNU General Public License
 ;    along with this program.  If not, see <http://www.gnu.org/licenses/>.

(ns cascalog.rules
  (:use [cascalog vars util graph])
  (:use clojure.contrib.set)
  (:use [clojure.set :only [intersection union difference]])
  (:use [clojure.contrib.set :only [subset?]])
  (:use [clojure.contrib.seq-utils :only [group-by find-first separate flatten]])
  (:require [cascalog [workflow :as w] [predicate :as p]])
  (:import [cascading.tap Tap])
  (:import [cascading.tuple Fields])
  (:import [cascading.flow Flow FlowConnector])
  (:import [cascading.pipe Pipe])
  (:import [cascalog CombinerSpec ClojureCombiner ClojureCombinedAggregator])
  (:import [java.util ArrayList]))

;; TODO:

;; 1. parameterized rules? - how does aggregation & joins work in this context? ->
;;    -> maybe just creates a generator...?
;; 
;; TODO: make it possible to create ungrounded rules that take in input vars (for composition)
;; i.e. (<- [?a ?b :> ?c] (func1 ?a :> ?c) (func2 ?b :> ?c))
;; (<- [?p] (data ?p ?a ?b) (func1 ?a :> ?c1) (func2 ?b :> ?c2) (= ?c1 ?c2))
;; (<- [?p] (age ?p ?a) (friend ?p1 ?p2) (= ?p ?p1))
;; (<- [?num] (nums ?num ?num) (source2 ?num))
;; (<- [?num] (nums ?num1 ?num2) (source2 ?num3) (= ?num1 ?num2 ?num3))
;; -> do filter equalities as you can, then do joins when they're valid
;; (<- [?p1 ?p2] (age ?p1 ?a) (age ?p2 ?a) (friend ?p1 ?p2))
;; (<- [?p1 ?p2] (age ?p1 ?a) (age ?p2 ?a1) (friend ?p1 ?p2) (= ?a ?a1))


;; infields for a join are the names of the join fields
(p/defpredicate join :infields)
(p/defpredicate group :assembly :infields :totaloutfields)

;; returns [generators operations aggregators]
(defn- split-predicates [predicates]
  (let [{ops :operation
         aggs :aggregator
         gens :generator
         opts :option} (merge {:operation [] :aggregator [] :generator [] :option []}
                                (group-by :type predicates))]
    (when (and (> (count aggs) 1) (some (complement :composable) aggs))
      (throw (IllegalArgumentException. "Cannot use both aggregators and buffers in same grouping")))
    [gens ops aggs opts] ))

(defstruct tailstruct :ground? :operations :drift-map :available-fields :node)

(defn- connect-op [tail op]
  (let [new-node (connect-value (:node tail) op)
        new-outfields (concat (:available-fields tail) (:outfields op))]
        (struct tailstruct (:ground? tail) (:operations tail) (:drift-map tail) new-outfields new-node)))

(defn- add-op [tail op]
  (let [tail (connect-op tail op)
        new-ops (remove-first (partial = op) (:operations tail))]
        (merge tail {:operations new-ops})))

(defn- op-allowed? [ground? available-fields op]
  (let [infields-set (set (:infields op))]
    (and (subset? infields-set (set available-fields))
         (or ground? (every? ground-var? infields-set)))
    ))

(defn- fixed-point-operations
  "Adds operations to tail until can't anymore. Returns new tail"
  [tail]
    (if-let [op (find-first (partial op-allowed? (:ground? tail) (:available-fields tail)) (:operations tail))]
      (recur (add-op tail op))
      tail ))

;; TODO: refactor and simplify drift algorithm
(defn- add-drift-op [tail equality-sets rename-map new-drift-map]
  (let [eq-assemblies (map w/equal equality-sets)
        outfields (vec (keys rename-map))
        rename-in (vec (vals rename-map))
        rename-assembly (if-not (empty? rename-in) (w/identity rename-in :fn> outfields :> Fields/SWAP) identity)
        assembly   (apply w/compose-straight-assemblies (concat eq-assemblies [rename-assembly]))
        infields (vec (apply concat rename-in equality-sets))
        tail (connect-op tail (p/predicate p/operation assembly infields outfields))
        newout (difference (set (:available-fields tail)) (set rename-in))]
        (merge tail {:drift-map new-drift-map :available-fields newout} )))

(defn- determine-drift [drift-map available-fields]
  (let [available-set (set available-fields)
        rename-map  (reduce (fn [m f]
                      (let [drift (drift-map f)]
                        (if (and drift (not (available-set drift)))
                          (assoc m drift f)
                          m )))
                      {} available-fields)
        eqmap      (select-keys (reverse-map (select-keys drift-map available-fields)) available-fields)
        equality-sets (map (fn [[k v]] (conj v k)) eqmap)
        new-drift-map (apply dissoc drift-map (apply concat (vals rename-map) equality-sets))]
    [new-drift-map equality-sets rename-map] ))

(defn- add-ops-fixed-point
  "Adds operations to tail until can't anymore. Returns new tail"
  [tail]
  (let [tail (fixed-point-operations tail)
        drift-map (:drift-map tail)
        [new-drift-map equality-sets rename-map] (determine-drift drift-map (:available-fields tail))]
    (if (and (empty? equality-sets) (empty? rename-map))
      tail
      (recur (add-drift-op tail equality-sets rename-map new-drift-map)))
    ))

(defn- tail-fields-intersection [& tails]
  (intersection (map #(set (:available-fields %)) tails)))

(defn- joinable? [joinfields tail]
  (let [join-set (set joinfields)
        tailfields (set (:available-fields tail))]
    (and (subset? join-set tailfields)
        (or (:ground? tail)
            (every? unground-var? (difference tailfields join-set))
        ))))

(defn- find-join-fields [tail1 tail2]
  (let [set1 (set (:available-fields tail1))
        set2 (set (:available-fields tail2))
        join-set (intersection set1 set2)]
  (if (every? (partial joinable? join-set) [tail1 tail2])
      join-set
      [] )))

(defn- select-join
  "Splits tails into [join-fields {join set} {rest of tails}]
   this is unoptimal. it's better to rewrite this as a search problem to find optimal joins"
  [tails]
  (let [pairs     (all-pairs tails)
        sections  (map (fn [[t1 t2]]
                          (find-join-fields t1 t2))
                      pairs)
        max-join  (last (sort-by count sections))]
      (when (empty? max-join) (throw (IllegalArgumentException. "Unable to join predicates together")))
      (cons (vec max-join) (separate (partial joinable? max-join) tails))
    ))

(defn- intersect-drift-maps [drift-maps]
  (let [drift-sets (map #(set (seq %)) drift-maps)
        tokeep     (apply intersection drift-sets)]
    (pairs2map (seq tokeep))))

(defn- merge-tails [graph tails]
  (let [tails (map add-ops-fixed-point tails)]
    (if (= 1 (count tails))
      (add-ops-fixed-point (merge (first tails) {:ground? true}))  ; if still unground, allow operations to be applied
      (let [[join-fields join-set rest-tails] (select-join tails)
            join-node             (create-node graph (p/predicate join join-fields))
            available-fields      (vec (set (apply concat (map :available-fields join-set))))
            new-ops               (vec (apply intersection (map #(set (:operations %)) join-set)))
            new-drift-map         (intersect-drift-maps (map :drift-map join-set))]
        (dorun (map #(create-edge (:node %) join-node) join-set))
        (recur graph (cons (struct tailstruct (some? :ground? join-set) new-ops new-drift-map
                              available-fields join-node) rest-tails))
        ))))

(defn- agg-available-fields [grouping-fields aggs]
  (vec (union (set grouping-fields) (apply union (map :outfields aggs)))))

(defn- agg-infields [aggs]
  (vec (apply union (map :infields aggs))))

(defn- normalize-grouping
  "Returns [new-grouping-fields inserter-assembly]"
  [grouping-fields]
  (if-not (empty? grouping-fields)
          [grouping-fields identity]
          (let [newvar (gen-nullable-var)]
            [[newvar] (w/insert newvar 1)]
            )))

(defn- specify-parallel-agg [agg]
  (let [pagg (:parallel-agg agg)]
    (CombinerSpec. (w/fn-spec (:init-var pagg)) (w/fn-spec (:combine-var pagg)))))

(defn- mk-combined-aggregator [combiner-spec arg outfield]
  (w/raw-every (w/fields arg) (ClojureCombinedAggregator. outfield (. combiner-spec combiner_spec)) Fields/ALL))

(defn mk-agg-arg-fields [fields]
  (if (empty? fields) nil (w/fields fields)))

(defn- build-agg-assemblies
  "returns [pregroup vec, postgroup vec]"
  [grouping-fields aggs]
  (if (every? :parallel-agg aggs)
    (let [argfields (map #(mk-agg-arg-fields (:infields %)) aggs)
          tempfields (take (count aggs) (repeatedly gen-nullable-var))
          specs (map specify-parallel-agg aggs)
          combiner (ClojureCombiner. (w/fields grouping-fields)
                                     argfields
                                     tempfields
                                     specs)]
          [[(w/raw-each Fields/ALL combiner Fields/RESULTS)]
           (map mk-combined-aggregator specs tempfields (map #(:outfield (:parallel-agg %)) aggs))] )
    [[identity] (map :serial-agg-assembly aggs)] ))

(defn- mk-group-by [grouping-fields options]
  (let [s (:sort options)]
   (if s
      (w/group-by grouping-fields (:sort options) (:reverse options))
      (w/group-by grouping-fields))))

(defn- build-agg-tail [options prev-tail grouping-fields aggs]
  (when (and (empty? aggs) (:sort options))
    (throw (IllegalArgumentException. "Cannot specify a sort when there are no aggregators" )))
  (if (and (not (:distinct options)) (empty? aggs))
    prev-tail
    (let [aggs              (if-not (empty? aggs) aggs [p/distinct-aggregator])
          [grouping-fields
           inserter]        (normalize-grouping grouping-fields)
          prev-node    (:node prev-tail)
          [prep-aggs postgroup-aggs] (build-agg-assemblies grouping-fields aggs)
          assem        (apply w/compose-straight-assemblies (concat
                         [inserter]
                         (map :pregroup-assembly aggs)
                         prep-aggs
                         [(mk-group-by grouping-fields options)]
                         postgroup-aggs
                         (map :post-assembly aggs)
                       ))
        total-fields (agg-available-fields grouping-fields aggs)
        node         (create-node (get-graph prev-node)
                        (p/predicate group assem (agg-infields aggs) total-fields))]
     (create-edge prev-node node)
     (struct tailstruct (:ground? prev-tail) (:operations prev-tail) (:drift-map prev-tail) total-fields node))
    ))

(defn projection-fields [needed-vars allfields]
  (let [needed-set (set needed-vars)
        all-set    (set allfields)
        inter      (intersection needed-set all-set)]
        (cond (= inter needed-set) needed-vars  ; maintain ordering when =, this is for output of generators to match declared ordering
              (empty? inter) [(first allfields)]  ; this happens during global aggregation, need to keep one field in
              true (vec inter)
        )))

(defn- mk-projection-assembly [forceproject projection-fields allfields]
    (if (and (not forceproject) (= (set projection-fields) (set allfields)))
      identity
      (w/select projection-fields)))

(defmulti node->generator (fn [pred & rest] (:type pred)))

(defmethod node->generator :generator [pred prevgens]
  (when (not-empty prevgens)
    (throw (RuntimeException. "Planner exception: Generator has inbound nodes")))
  pred )

(w/defmapop [join-fields-selector [num-fields]] [& args]
  (let [joins (partition num-fields args)]
    (if-let [join-fields (find-first (partial some? (complement nil?)) joins)]
      join-fields
      (repeat num-fields nil))))

(defn- replace-join-fields [join-fields join-renames fields]
  (let [replace-map (zipmap join-fields join-renames)]
    (reduce (fn [ret f]
      (let [newf (replace-map f)
            newf (if newf newf f)]
            (conj ret newf)))
      [] fields)))

(defn- generate-join-fields [numfields numpipes]
  (take numpipes (repeatedly (partial gen-nullable-vars numfields))))

(defmethod node->generator :join [pred prevgens]
  (let [join-fields (:infields pred)
        num-join-fields (count join-fields)
        sourcemap   (apply merge (map :sourcemap prevgens))
        [inner-gens
        outer-gens] (separate :ground? prevgens)
        prevgens    (concat inner-gens outer-gens)
        infields    (map :outfields prevgens)
        inpipes     (map (fn [p f] (w/assemble p (w/select f))) (map :pipe prevgens) infields)
        join-renames (generate-join-fields num-join-fields (count prevgens))
        rename-fields (flatten (map (partial replace-join-fields join-fields) join-renames infields))
        keep-fields (vec (set (apply concat infields)))
        mixjoin     (concat (repeat (count inner-gens) true)
                            (repeat (count outer-gens) false))
        joined      (w/assemble inpipes (w/co-group (repeat (count inpipes) join-fields)
                                              rename-fields (w/mixed-joiner mixjoin))
                                        (join-fields-selector [num-join-fields]
                                          (flatten join-renames) :fn> join-fields :> Fields/SWAP))]
        (p/predicate p/generator true sourcemap joined keep-fields)
    ))

(defmethod node->generator :operation [pred prevgens]
  (when-not (= 1 (count prevgens))
    (throw (RuntimeException. "Planner exception: operation has multiple inbound generators")))
  (let [prevpred (first prevgens)]
    (merge prevpred {:outfields (concat (:outfields pred) (:outfields prevpred))
            :pipe ((:assembly pred) (:pipe prevpred))})))

(defmethod node->generator :group [pred prevgens]
  (when-not (= 1 (count prevgens))
    (throw (RuntimeException. "Planner exception: group has multiple inbound generators")))
  (let [prevpred (first prevgens)]
    (merge prevpred {:outfields (:totaloutfields pred)
            :pipe ((:assembly pred) (:pipe prevpred))})))

;; forceproject necessary b/c *must* reorder last set of fields coming out to match declared ordering
(defn build-generator [forceproject needed-vars node]
  (let [pred           (get-value node)
        my-needed      (vec (set (concat (:infields pred) needed-vars)))
        prev-gens      (doall (map (partial build-generator false my-needed) (get-inbound-nodes node)))
        newgen         (node->generator pred prev-gens)
        project-fields (projection-fields needed-vars (:outfields newgen)) ]
        (when (and forceproject (not= project-fields needed-vars))
          (throw (RuntimeException. (str "Only able to build to " project-fields " but need " needed-vars))))
        (merge newgen {:pipe ((mk-projection-assembly forceproject project-fields (:outfields newgen)) (:pipe newgen))
                :outfields project-fields})))

(def DEFAULT-OPTIONS
  {:distinct true
   :sort nil
   :reverse false})

(defn- mk-options [opt-predicates]
  (apply merge DEFAULT-OPTIONS (map (fn [p] {(:key p) (:val p)}) opt-predicates)))

(defn- validate-vars! [out-vars predicate-vars]
  (let [check-fn (fn [unboundset v]
      (if (unboundset v)
        (throw (IllegalArgumentException. "Cannot use ungrounding var as an output variable twice"))
        (conj unboundset v)
        ))]
    (reduce check-fn #{} (mapcat second predicate-vars))
    ))

(defn build-rule [out-vars raw-predicates]
  (validate-vars! out-vars (map (fn [p] [(filter cascalog-var? (get p :infields []))
                                        (filter cascalog-var? (get p :outfields []))])
                            raw-predicates))
  (let [[_ out-vars vmap]     (uniquify-vars [] out-vars {})
        update-fn             (fn [[preds vmap] [op opvar vars]]
                                (let [{invars :in outvars :out} (p/parse-variables vars (p/predicate-default-var op))
                                      [invars outvars vmap] (uniquify-vars invars outvars vmap)]
                                  [(conj preds [op opvar invars outvars]) vmap] ))
        [raw-predicates vmap] (reduce update-fn [[] vmap] raw-predicates)
        drift-map             (mk-drift-map vmap)
        [gens ops aggs
          opt-predicates]     (split-predicates (map (partial apply p/build-predicate) raw-predicates))
        options               (mk-options opt-predicates)
        rule-graph            (mk-graph)
        tails                 (map (fn [g] (struct tailstruct (:ground? g)
                                        ops drift-map (:outfields g) (create-node rule-graph g))) gens)
        joined                (merge-tails rule-graph tails)
        grouping-fields       (seq (intersection (set (:available-fields joined)) (set out-vars)))
        agg-tail              (build-agg-tail options joined grouping-fields aggs)
        tail                  (add-ops-fixed-point agg-tail)]
      (when (not-empty (:operations tail))
        (throw (RuntimeException. (str "Could not apply all operations " (:operations tail)))))
      (build-generator true out-vars (:node tail))))

(defn mk-raw-predicate [pred]
  (let [[op-sym & vars] pred
        str-vars (vars2str vars)]
    [op-sym (try-resolve op-sym) str-vars]))

(defn connect-to-sink [gen sink]
    (let [sink-fields (.getSinkFields sink)
          pipe        (:pipe gen)
          pipe        (if-not (.isDefined sink-fields)
                        (if (.isAll sink-fields)
                          pipe
                          (throw (IllegalArgumentException.
                            "Cannot sink to a sink with meta fields defined besides Fields/ALL")))
                        ((w/identity (:outfields gen) :fn> sink-fields) pipe))]
          ((w/pipe-rename (uuid)) pipe)))

