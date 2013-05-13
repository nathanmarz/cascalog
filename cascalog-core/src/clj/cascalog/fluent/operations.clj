(ns cascalog.fluent.operations
  (:require [clojure.tools.macro :refer (name-with-attributes)]
            [clojure.set :refer (subset?)]
            [cascalog.fluent.conf :as conf]
            [cascalog.fluent.cascading :refer (fields default-output)]
            [cascalog.fluent.algebra :refer (plus)]
            [cascalog.util :as u]
            [cascalog.fluent.source :as src]
            [hadoop-util.core :as hadoop]
            [jackknife.seq :refer (unweave)])
  (:import [java.io File]
           [cascading.tuple Tuple Fields]
           [cascalog.ops KryoInsert]
           [cascading.operation Identity Debug NoOp]
           [cascading.operation.filter Sample]
           [cascading.operation.aggregator First Count Sum Min Max]
           [cascading.pipe Pipe Each Every GroupBy CoGroup Merge]
           [cascading.pipe.joiner InnerJoin]
           [cascading.pipe.assembly Rename]
           [cascalog ClojureFilter ClojureMapcat ClojureMap
            ClojureAggregator ClojureBuffer ClojureBufferIter
            FastFirst MultiGroupBy ClojureMultibuffer]))

;; ## Cascalog Function Representation

(defn ns-fn-name-pair [v]
  (let [m (meta v)]
    [(str (:ns m)) (str (:name m))]))

(defn fn-spec
  "v-or-coll => var or [var & params]

   Returns an Object array that is used to represent a Clojure
   function. If the argument is a var, the array represents that
   function. If the argument is a coll, the array represents the
   function returned by applying the first element, which should be a
   var, to the rest of the elements."
  [v-or-coll]
  (cond
   (var? v-or-coll)
   (into-array Object (ns-fn-name-pair v-or-coll))

   (coll? v-or-coll)
   (into-array Object
               (concat
                (ns-fn-name-pair (clojure.core/first v-or-coll))
                (next v-or-coll)))

   :else (throw (IllegalArgumentException. (str v-or-coll)))))

;; ## Operations
;;
;; TODO: Note that scalding uses a form of "let" for stateful
;; operations. They implement stateful operations with a context
;; object. Ask Oscar -- what's the context object? Looks like we can
;; use this to get around serialization.

(defn add-op
  "Accepts a flow and a function from pipe to pipe and applies the
  operation to the active head pipe."
  [flow fn]
  (update-in flow [:pipe] fn))

(defmacro defop
  "Defines a flow operation."
  [f-name & tail]
  (let [[f-name [args & body]] (name-with-attributes f-name tail)]
    `(defn ~f-name
       {:arglists '([~'flow ~@args])}
       [flow# ~@args]
       (add-op flow# ~@body))))

(defop each
  "Accepts a flow, a function from result fields => cascading
  Function, input fields and output fields and returns a new flow."
  [f from-fields to-fields]
  (let [from-fields (fields from-fields)
        to-fields   (fields to-fields)]
    (fn [pipe]
      (Each. pipe
             from-fields
             (f to-fields)
             (default-output from-fields to-fields)))))

(defn rename-pipe
  ([flow] (rename-pipe flow (u/uuid)))
  ([flow name]
     (add-op flow #(Pipe. name %))))

;; TODO: Make sure this still works with new "fields" name.
(defop select*
  "Remove all but the supplied fields from the given flow."
  [keep-fields]
  #(Each. % (fields keep-fields)
          (Identity. keep-fields)))

(defop discard*
  "Discard the supplied fields."
  [drop-fields]
  #(Each. % drop-fields (NoOp.) Fields/SWAP))

(defop debug*
  "Prints all tuples that pass through the StdOut."
  []
  #(Each. % (Debug.)))

(defn insert*
  "Accepts a flow and alternating field/value pairs and inserts these
  items into the flow."
  [flow & field-v-pairs]
  (let [[out-fields vals] (unweave field-v-pairs)]
    (each flow #(KryoInsert. % (into-array Object vals))
          Fields/NONE out-fields)))

(defn sample*
  "Sample some percentage of elements within this pipe. percent should
   be between 0.00 (0%) and 1.00 (100%) you can provide a seed to get
   reproducible results."
  ([flow percent]
     (add-op flow #(Each. % (Sample. percent))))
  ([flow percent seed]
     (add-op flow #(Each. % (Sample. percent seed)))))

(defn rename*
  "rename old-fields to new-fields."
  ([flow new-fields]
     (rename* flow Fields/ALL new-fields))
  ([flow old-fields new-fields]
     (add-op flow #(Rename. %
                            (fields old-fields)
                            (fields new-fields)))))

(defop filter* [op-var in-fields]
  #(->> (ClojureFilter. (fn-spec op-var) false)
        (Each. % (fields in-fields))))

(defn map* [flow op-var in-fields out-fields]
  (each flow #(ClojureMap. % (fn-spec op-var) false)
        in-fields
        out-fields))

(defn mapcat* [flow op-var in-fields out-fields]
  (each flow #(ClojureMapcat. % (fn-spec op-var) false)
        in-fields
        out-fields))

(defn merge*
  "Merges the supplied flows."
  [& flows]
  (reduce plus flows))

;; ## Aggregations
;;
;; One can implement a groupAll by leaving group-fields nil. Cascalog
;;will use a random field and group on a 1:

(comment
  "from rules.clj. build-agg-assemblies in that same namespace has the
  rules for how to actually build aggregators, and how to choose which
  type of aggregators to use."
  "my-group-by creates a group by operation with proper respect for
  fields and sorting."

  (defn- normalize-grouping
    "Returns [new-grouping-fields inserter-assembly]. If no grouping
  fields are supplied, ths function groups on 1, which forces a global
  grouping."
    [grouping-fields]
    (if (seq grouping-fields)
      [grouping-fields identity]
      (let [newvar (v/gen-nullable-var)]
        [[newvar] (w/insert newvar 1)]))))

(defrecord GroupBuilder
    [flow reducers sort-fields group-fields reverse?])

;; ## Output Operations
;;
;; This section covers output and traps

(defn in-branch
  "Accepts a temporary name and a function from flow => flow and
  performs the operation within a renamed branch."
  ([flow f]
     (in-branch flow (u/uuid) f))
  ([flow name f]
     (-> flow
         (rename-pipe name)
         (f name)
         (rename-pipe))))

(defn write* [flow sink]
  (let [sink (src/to-sink sink)]
    (-> flow
        (in-branch (.getIdentifier sink)
                   (fn [subflow name]
                     (-> subflow
                         (update-in [:tails] conj (:pipe subflow))
                         (update-in [:sink-map] assoc name sink)))))))

(defn trap*
  "Applies a trap to the current branch of the supplied flow."
  [flow trap]
  (let [trap (src/to-sink trap)
        id   (.getIdentifier trap)]
    (-> flow
        (rename-pipe id)
        (update-in [:trap-map] assoc id trap))))

;; TODO: Figure out if I really understand what's going on with the
;; trap options. Do this by testing the traps with a few throws inside
;; and one after. Make sure the throw after causes a failure, but not
;; inside.

(defn with-trap*
  "Applies a trap to everything that occurs within the supplied
  function of flow => flow."
  [flow trap f]
  (-> flow (trap* trap) f (rename-pipe)))
