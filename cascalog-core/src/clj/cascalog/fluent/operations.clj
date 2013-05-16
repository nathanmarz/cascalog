(ns cascalog.fluent.operations
  (:import [cascalog ClojureMonoidFunctor])
  (:require [clojure.tools.macro :refer (name-with-attributes)]
            [clojure.set :refer (subset? difference)]
            [cascalog.fluent.conf :as conf]
            [cascalog.fluent.cascading :as casc :refer (fields default-output)]
            [cascalog.fluent.algebra :refer (plus)]
            [cascalog.fluent.fn :as serfn]
            [cascalog.util :as u]
            [cascalog.fluent.source :as src]
            [hadoop-util.core :as hadoop]
            [jackknife.core :refer (throw-illegal)]
            [jackknife.seq :refer (unweave collectify)])
  (:import [java.io File]
           [cascading.tuple Tuple Fields]
           [cascalog.ops KryoInsert]
           [cascading.operation Identity Debug NoOp]
           [cascading.operation.filter Sample]
           [cascading.operation.aggregator First Count Sum Min Max]
           [cascading.pipe Pipe Each Every GroupBy CoGroup Merge ]
           [cascading.pipe.joiner InnerJoin]
           [cascading.pipe.assembly Rename AggregateBy]
           [cascalog ClojureFilter ClojureMapcat ClojureMap
            ClojureAggregator ClojureBuffer ClojureBufferIter
            FastFirst MultiGroupBy ClojureMultibuffer ClojureMonoidAggregator
            ClojureMonoidAggregator ClojureMonoidFunctor]))

;; ## Cascalog Function Representation

(defn fn-spec [var] (serfn/serialize var))

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

(defn identity*
  "Mirrors the supplied set of input fields into the output fields."
  [flow input output]
  (each flow #(Identity. %) input output))

(defop discard*
  "Discard the supplied fields."
  [drop-fields]
  #(Each. % drop-fields (NoOp.) Fields/SWAP))

(defn replace-dups
  "Accepts a sequence and a (probably stateful) generator and returns
  a new sequence with all duplicates replaced by a call to `gen`."
  [coll gen]
  (second
   (reduce (fn [[seen-set acc] elem]
             (if (contains? seen-set elem)
               [seen-set (conj acc (gen))]
               [(conj seen-set elem) (conj acc elem)]))
           [#{} []]
           (into [] coll))))

(defn with-dups
  "Accepts a flow, some fields, and a function from (flow,
  unique-fields, new-fields) => flow and appropriately handles
  duplicate entries inside of the fields.

  The fields passed to the supplied function will be guaranteed
  unique. New fields are passed as a third option to the supplying
  function, which may decide to call (discard* delta) if the fields
  are still around."
  [flow from-fields f]
  (let [from-fields (collectify from-fields)]
    (if (apply distinct? from-fields)
      (f flow from-fields [])
      (let [cleaned-fields (replace-dups from-fields casc/gen-unique-var)
            delta (seq (difference (set cleaned-fields)
                                   (set from-fields)))]
        (-> (reduce (fn [subflow [field gen]]
                      (if (= field gen)
                        subflow
                        (identity* subflow field gen)))
                    flow
                    (map vector from-fields cleaned-fields))
            (f cleaned-fields delta))))))

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

(defn filterop [op]
  (fn [flow & more]
    (apply filter* flow op more)))

(defn map* [flow op-var in-fields out-fields]
  (each flow #(ClojureMap. % (fn-spec op-var) false)
        in-fields
        out-fields))

(defn mapop [op]
  (fn [flow & more]
    (apply map* flow op more)))

(defn mapcat* [flow op-var in-fields out-fields]
  (each flow #(ClojureMapcat. % (fn-spec op-var) false)
        in-fields
        out-fields))

(defn mapcatop [op]
  (fn [flow & more]
    (apply mapcat* flow op more)))

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

(defrecord GroupBuilder [flow reducers sort-fields group-fields reverse?])

(defprotocol AggregatorBuilder
  (gen-agg [_]))

(defprotocol ParallelAggregatorBuilder
  (gen-functor [_]))

(defprotocol BufferBuilder
  (gen-buffer [_]))

(defn agg
  [agg-fn in-fields out-fields]
  (let [in-fields  (fields in-fields)
        out-fields (fields out-fields)]
    (reify AggregatorBuilder
      (gen-agg [_]
        {:input in-fields
         :op (ClojureAggregator. out-fields (fn-spec agg-fn) false)}))))

(defn par-agg
  [agg-fn in-fields out-fields]
  (let [in-fields (fields in-fields)
        out-fields (fields out-fields)
        spec (fn-spec agg-fn)]
    (reify
      ParallelAggregatorBuilder
      (gen-functor [_]
        (ClojureMonoidFunctor. out-fields spec false))
      AggregatorBuilder
      (gen-agg [_]
        {:input in-fields
         :op (ClojureMonoidAggregator. out-fields spec false)}))))


(defn bufferiter
  [buffer-fn in-fields out-fields]
  (let [in-fields  (fields in-fields)
        out-fields (fields out-fields)]
    (reify BufferBuilder
      (gen-buffer [_]
        {:input in-fields
         :op (ClojureBufferIter. out-fields (fn-spec buffer-fn) false)}))))

(defn buffer
  [buffer-fn in-fields out-fields]
  (let [in-fields  (fields in-fields)
        out-fields (fields out-fields)]
    (reify BufferBuilder
      (gen-buffer [_]
        {:input in-fields
         :op (ClojureBuffer. out-fields (fn-spec buffer-fn) false)}))))

(defn is-agg?
  [agg-type]
  (partial satisfies? agg-type))

;; TODO: add options
(defn group-by*
  [flow group-fields & aggs]
  (let [group-fields (fields group-fields)]
    (add-op flow
            (fn [pipe]
              (cond
               (some (is-agg? BufferBuilder) aggs)
               ;; emit buffer
               (do (when (> (count aggs) 1)
                     (throw-illegal "Buffer must be specified on its own"))
                   (let [{:keys [input op]} (gen-buffer (first aggs))]
                     (-> pipe
                         (GroupBy. group-fields)
                         (Every. input op))))

               (every? (is-agg? ParallelAggregatorBuilder) aggs)
               ;; emit AggregateBy
               (let [aggs (for [agg-pred aggs
                                :let [{:keys [input op]} (gen-agg agg-pred)]]
                            (AggregateBy. input
                                          (gen-functor agg-pred)
                                          op))]
                 (AggregateBy. pipe group-fields (into-array AggregateBy aggs)))

               :else
               ;; simple aggregation
               (let [pipe (GroupBy. pipe group-fields)]
                 (reduce (fn [p agg]
                           (let [{:keys [op input]} (gen-agg agg)]
                             (Every. p input op)))
                         pipe aggs)))))))

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
