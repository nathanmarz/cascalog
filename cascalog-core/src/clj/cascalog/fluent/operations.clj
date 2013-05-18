(ns cascalog.fluent.operations
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
           [cascading.tuple Fields]
           [cascading.operation Identity Debug NoOp]
           [cascading.operation.filter Sample]
           [cascading.operation.aggregator First Count Sum Min Max]
           [cascading.pipe Pipe Each Every GroupBy CoGroup Merge ]
           [cascading.pipe.joiner InnerJoin]
           [cascading.pipe.assembly Rename AggregateBy]
           [cascalog ClojureFilter ClojureMapcat ClojureMap
            ClojureBuffer ClojureBufferIter FastFirst
            MultiGroupBy ClojureMultibuffer]
           [cascalog.aggregator ClojureAggregator
            ClojureMonoidAggregator ClojureMonoidFunctor
            ClojureAggregateBy CombinerSpec]))

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
  #(->> (ClojureFilter. op-var)
        (Each. % (fields in-fields))))

(defn map* [flow op-var in-fields out-fields]
  (each flow #(ClojureMap. % op-var)
        in-fields
        out-fields))

(defn mapcat* [flow op-var in-fields out-fields]
  (each flow #(ClojureMapcat. % op-var)
        in-fields
        out-fields))

(defn merge*
  "Merges the supplied flows."
  [& flows]
  (reduce plus flows))

;; ## Aggregations
;;
;; TODO: Convert away from this protocol approach. Redefining these
;; namespaces causes issues, when the aggregators suddenly aren't
;; instances and don't respond to isa?
;;
;; We need to make sure that we can jack in other aggregators from
;;Cascading directly, or at least let the user chain a raw every and a
;;raw groupBy. These should take appropriate keyword options.

(defprotocol ParallelAggregatorBuilder
  (gen-functor [_]))

(defprotocol AggregatorBuilder
  (gen-agg [_]))

(defprotocol BufferBuilder
  (gen-buffer [_]))

(defn par-agg
  "Creates a parallel aggregation operation. TODO: Take a prepare and
  present var."
  [agg-fn in-fields out-fields]
  (let [in-fields  (fields in-fields)
        out-fields (fields out-fields)
        spec (CombinerSpec. agg-fn)]
    (reify
      ParallelAggregatorBuilder
      (gen-functor [_]
        (ClojureMonoidFunctor. out-fields spec))
      AggregatorBuilder
      (gen-agg [_]
        {:input in-fields
         :op (ClojureMonoidAggregator. out-fields spec)}))))

(defn agg
  [agg-fn in-fields out-fields]
  (let [in-fields  (fields in-fields)
        out-fields (fields out-fields)]
    (reify AggregatorBuilder
      (gen-agg [_]
        {:input in-fields
         :op (ClojureAggregator. out-fields agg-fn)}))))

(defn bufferiter
  [buffer-fn in-fields out-fields]
  (let [in-fields  (fields in-fields)
        out-fields (fields out-fields)]
    (reify BufferBuilder
      (gen-buffer [_]
        {:input in-fields
         :op (ClojureBufferIter. out-fields buffer-fn)}))))

(defn buffer
  [buffer-fn in-fields out-fields]
  (let [in-fields  (fields in-fields)
        out-fields (fields out-fields)]
    (reify BufferBuilder
      (gen-buffer [_]
        {:input in-fields
         :op (ClojureBuffer. out-fields buffer-fn)}))))

(defn is-agg?
  [agg-type]
  (partial satisfies? agg-type))

;; TODO: add options

(def REDUCER-KEY "mapred.reduce.tasks")

(defn set-reducers
  "Set the number of reducers for this step in the pipe."
  [pipe reducers]
  (if-not reducers
    pipe
    (cond (pos? reducers)
          (do (-> pipe
                  (.getStepConfigDef)
                  (.setProperty REDUCER-KEY, (str reducers)))
              pipe)
          (= -1 reducers) pipe
          :else (throw-illegal "Number of reducers must be non-negative."))))

;; TODO: Add proper assertions around sorting. (We can't sort when
;; we're in AggregateBy, for example.

;; TODO: Add a "reduce-mode" function that returns a keyword
;; representing the proper aggregation.
;;
;; TODO: Split these out into a raw every, a mk-group-by kind of like
;; what Nathan had below, and a raw aggregateby. Then we need an
;; operation to determine which works.

(comment
  "from rules.clj:"
  (defn- mk-group-by
    "Create a groupby operation, respecting grouping and sorting
  options."
    [grouping-fields options]
    (let [{s :sort rev :reverse} options]
      (if (seq s)
        (w/group-by grouping-fields s rev)
        (w/group-by grouping-fields))))

  (defn- build-agg-assemblies
    "returns [pregroup vec, postgroup vec]"
    [grouping-fields aggs]
    (cond (and (= 1 (count aggs))
               (:parallel-agg (first aggs))
               (:buffer? (first aggs)))
          (mk-parallel-buffer-agg grouping-fields (first aggs))

          (every? :parallel-agg aggs) (mk-parallel-aggregator grouping-fields aggs)

          :else [[identity] (map :serial-agg-assembly aggs)])))

(defn group-by*
  [flow group-fields aggs
   & {:keys [reducers spill-threshold sort-fields reverse? reduce-only]
      :or {spill-threshold 10000}}]
  (let [group-fields  (fields group-fields)
        build-groupby (fn [pipe every-seq]
                        (let [pipe (if sort-fields
                                     (GroupBy. pipe group-fields
                                               (fields sort-fields)
                                               (boolean reverse?))
                                     (GroupBy. pipe group-fields))]
                          (-> (reduce (fn [p {:keys [op input]}]
                                        (Every. p input op))
                                      pipe every-seq)
                              (set-reducers reducers))))]
    (add-op flow
            (fn [pipe]
              (cond
               (some (is-agg? BufferBuilder) aggs)
               ;; emit buffer
               (do (when (> (count aggs) 1)
                     (throw-illegal "Buffer must be specified on its own"))
                   (build-groupby pipe (map gen-buffer aggs)))

               (and (not reduce-only)
                    (every? (is-agg? ParallelAggregatorBuilder) aggs))
               ;; emit AggregateBy
               (let [aggs (for [agg-pred aggs
                                :let [{:keys [input op]} (gen-agg agg-pred)]]
                            (ClojureAggregateBy. input
                                                 (gen-functor agg-pred)
                                                 op))]
                 (AggregateBy. pipe group-fields spill-threshold
                               (into-array AggregateBy aggs)))

               :else (build-groupby pipe (map gen-agg aggs)))))))

(defn unique
  "Performs a unique on the input pipe by the supplied fields."
  ([flow]
     (unique flow Fields/ALL))
  ([flow unique-fields]
     (let [unique-fields (fields unique-fields)]
       (add-op flow (fn [pipe]
                      (-> pipe
                          (GroupBy. unique-fields)
                          (Every. (FastFirst.) Fields/RESULTS)))))))

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
;;
;; TODO: Add "checkpoint" function, injecting a checkpoint pipe.

(defn with-trap*
  "Applies a trap to everything that occurs within the supplied
  function of flow => flow."
  [flow trap f]
  (-> flow (trap* trap) f (rename-pipe)))
