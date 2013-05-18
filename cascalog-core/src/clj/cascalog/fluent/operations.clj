(ns cascalog.fluent.operations
  (:require [clojure.tools.macro :refer (name-with-attributes)]
            [clojure.set :refer (subset? difference intersection)]
            [cascalog.vars :as v]
            [cascalog.fluent.conf :as conf]
            [cascalog.fluent.cascading :as casc
             :refer (fields default-output)]
            [cascalog.fluent.algebra :refer (plus)]
            [cascalog.fluent.types :refer (generator to-sink)]
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
;; All of these operations work on implementers of the Generator
;; protocol, defined in cascalog.fluent.types.

(defn add-op
  "Accepts a generator and a function from pipe to pipe and applies
  the operation to the active head pipe."
  [flow fn]
  (update-in (generator flow)
             [:pipe]
             fn))

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
  ([gen] (rename-pipe gen (u/uuid)))
  ([gen name]
     (add-op gen #(Pipe. name %))))

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
  (reduce (fn [[seen-set acc] elem]
            (if (contains? seen-set elem)
              [seen-set (conj acc (gen))]
              [(conj seen-set elem) (conj acc elem)]))
          [#{} []]
          (collectify coll)))

(defn with-dups
  "Accepts a flow, some fields, and a function from (flow,
  unique-fields, new-fields) => flow and appropriately handles
  duplicate entries inside of the fields.

  The fields passed to the supplied function will be guaranteed
  unique. New fields are passed as a third option to the supplying
  function, which may decide to call (discard* delta) if the fields
  are still around."
  [flow from-fields f]
  (if (apply distinct? (collectify from-fields))
    (f flow from-fields [])
    (let [[delta cleaned-fields]
          (replace-dups from-fields casc/gen-unique-var)]
      (-> (reduce (fn [subflow [field gen]]
                    (if (= field gen)
                      subflow
                      (identity* subflow field gen)))
                  flow
                  (map vector from-fields cleaned-fields))
          (f cleaned-fields (seq delta))))))

;; ## Logic Variable Substitution Rules

;; TODO: If we have some sort of ignored variable coming out of a
;; Cascalog query, we want to strip all operations out at that
;; point. Probably when we're building up a generator.

(defn- constant-substitutions
  "Returns a 2-vector of the form

   [new variables, {map of newvars to values to substitute}]"
  [vars]
  (u/substitute-if (complement v/cascalog-var?)
                   (fn [_] (v/gen-nullable-var))
                   (collectify vars)))

(defn insert-subs [flow sub-m]
  (if (empty? sub-m)
    flow
    (apply insert* flow (u/flatten sub-m))))

(defn with-constants
  "Allows constant substitution on inputs."
  [gen in-fields f]
  (let [[new-input sub-m] (constant-substitutions in-fields)
        ignored (keys sub-m)]
    (-> (insert-subs gen sub-m)
        (f (fields new-input))
        (discard* (fields ignored)))))

(defn- replace-ignored-vars
  "Replaces all ignored variables with a nullable cascalog
  variable. "
  [vars]
  (map (fn [v] (if (= "_" v) (v/gen-nullable-var) v))
       (collectify vars)))

(defn not-nil? [& xs]
  (every? (complement nil?) xs))

(defn filter-nullable-vars
  "If there are any nullable variables present in the output, filter
  nulls out now."
  [flow fields]
  (if-let [non-null-fields (seq (filter v/non-nullable-var? fields))]
    (filter* flow #'not-nil? non-null-fields)
    flow))

(defn no-overlap? [large small]
  (empty?
   (intersection (set (collectify large))
                 (set (collectify small)))))

;; TODO: Replace 'build-predicate' with this thing.
;;
;; The enhance-predicate logic in predicate.clj sort of does this
;;"accept the function, do something around it" logic.

(defn logically
  "Accepts a flow, input fields, output fields and a function that
  accepts the same things and allows for the following features:

  Any variables not prefixed with !, !! or ? are treated as constants
  in the flow. This allows for (map* flow + 10 [\"?a\"] [\"?b\"]) to
  work properly and clean up its fields without hassle.

  Any non-nullable output variables (prefixed with ?) are removed from
  the flow.

  Duplicate input fields are allowed. It is currently NOT allowed to
  output one of the input variables. In Cascalog, this triggers an
  implicit filter; this needs to be supplied at another layer."
  [gen in-fields out-fields f]
  {:pre [(no-overlap? out-fields in-fields)]}
  (let [new-output (replace-ignored-vars out-fields)
        ignored (difference (set new-output)
                            (set (collectify out-fields)))]
    (with-constants gen in-fields
      (fn [gen in]
        (with-dups gen in
          (fn [gen in delta]
            (-> gen
                (f (fields in)
                   (fields new-output))
                (discard* (fields (concat delta ignored)))
                (filter-nullable-vars new-output))))))))

(comment
  "This one works, since all inputs are logical variables."
  (-> [1 2 3 4]
      (rename* "?a")
      (map* inc "?a" "?other")
      (logically ["?a" 10 "?other"] "?b"
                 (fn [gen in out]
                   (-> gen (map* * in out))))
      (to-memory))

  "This one fails, since one of the inputs is interpreted as a
  constant."
  (-> [1 2 3 4]
      (rename* "?a")
      (map* inc "?a" "other")
      (logically ["?a" 10 "other"] "?b"
                 (fn [gen in out]
                   (-> gen (map* * in out))))
      (to-memory))

  "TODO: in logically, we need to start enforcing some rules. no one
  can push an actual instance of Fields in at this point. It doesn't
  make sense with these logical rules. We do now have some notion of
  an assembly -- an assembly is a function from one flow to another.

  I think we need a typeclass to chain some operation. add-op
  shouldn't have to take a generator -- it can definitely augment just
  a pipe, or even another function that's going to modify a pipe (by
  composition)."

  ;; State monad to build these things up?
  ;; Or do we need a continuation passing style:
  (defn augment [a b]
    (fn [pipe]
      [])
    )
  (defprotocol IAssembly
    (augment [_ pipe]
      ))
  )

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

(defprotocol IAggregateBy
  (aggregate-by [_]))

(defprotocol IAggregator
  (add-aggregator [_ pipe]))

(defprotocol IBuffer
  (add-buffer [_ pipe]))

(defn parallel-agg
  "Creates a parallel aggregation operation. TODO: Take a prepare and
  present var."
  [agg-fn in-fields out-fields]
  (let [in-fields  (fields in-fields)
        out-fields (fields out-fields)
        spec       (CombinerSpec. agg-fn)
        aggregator (ClojureMonoidAggregator. out-fields spec)]
    (reify
      IAggregateBy
      (aggregate-by [_]
        (ClojureAggregateBy. in-fields
                             (ClojureMonoidFunctor. out-fields spec)
                             aggregator))
      IAggregator
      (add-aggregator [_ pipe]
        (Every. pipe in-fields aggregator)))))

(defn agg
  "Returns in instance of IAggregator that adds a reduce-side-only
  aggregation to its supplied pipe."
  [agg-fn in-fields out-fields]
  (let [in-fields  (fields in-fields)
        out-fields (fields out-fields)]
    (reify IAggregator
      (add-aggregator [_ pipe]
        (Every. pipe in-fields (ClojureAggregator. out-fields agg-fn))))))

(defn bufferiter
  [buffer-fn in-fields out-fields]
  (let [in-fields  (fields in-fields)
        out-fields (fields out-fields)]
    (reify IBuffer
      (add-buffer [_ pipe]
        (Every. pipe in-fields (ClojureBufferIter. out-fields buffer-fn))))))

(defn buffer
  [buffer-fn in-fields out-fields]
  (let [in-fields  (fields in-fields)
        out-fields (fields out-fields)]
    (reify IBuffer
      (add-buffer [_ pipe]
        (Every. pipe in-fields (ClojureBuffer. out-fields buffer-fn))))))

(defn aggregator? [x]
  (satisfies? IAggregator x))

(defn parallel-agg? [x]
  (satisfies? IAggregateBy x))

(defn buffer? [x]
  (satisfies? IBuffer x))

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

(defn aggregate-mode
  "Accepts a sequence of aggregators and a boolean force-reduce? flag
  and returns a keyword representing the aggregation type."
  [aggregators force-reduce?]
  (cond (some buffer? aggregators)
        (if (> (count aggregators) 1)
          (throw-illegal "Buffer must be specified on its own")
          ::buffer)

        (and (not force-reduce?)
             (every? parallel-agg? aggregators))
        ::parallel

        :else ::aggregate))

(defn- groupby
  "Adds a raw GroupBy operation to the pipe. Don't use this directly."
  [pipe group-fields sort-fields reverse?]
  (if sort-fields
    (GroupBy. pipe group-fields
              (fields sort-fields)
              (boolean reverse?))
    (GroupBy. pipe group-fields)))

(defn- aggby [pipe group-fields spill-threshold aggs]
  (let [aggs (->> aggs
                  (map aggregate-by)
                  (into-array AggregateBy))]
    (AggregateBy. pipe group-fields spill-threshold aggs)))

;; TODO: Add proper assertions around sorting. (We can't sort when
;; we're in AggregateBy, for example.

(defop group-by*
  "Applies a grouping operation to the supplied generator."
  [group-fields aggs
   & {:keys [reducers spill-threshold sort-fields reverse? reduce-only]
      :or {spill-threshold 10000}}]
  (fn [pipe]
    (let [group-fields (fields group-fields)
          build-group  (fn [thunk]
                         (thunk
                          (groupby pipe group-fields
                                   (fields sort-fields)
                                   reverse?)))
          mode (aggregate-mode aggs reduce-only)]
      (case mode
        ::buffer    (build-group #(add-buffer (first aggs) %))
        ::aggregate (build-group (fn [grouped]
                                   (reduce (fn [p op]
                                             (add-aggregator op p))
                                           grouped aggs)))
        ::parallel  (aggby pipe group-fields spill-threshold aggs)
        (throw-illegal "Unsupported aggregation mode: " mode)))))

(defn unique
  "Performs a unique on the input pipe by the supplied fields."
  ([flow]
     (unique flow Fields/ALL))
  ([flow unique-fields]
     (let [agg (reify IAggregator
                 (add-aggregator [_ pipe]
                   (Every. pipe (FastFirst.) Fields/RESULTS)))]
       (group-by* flow unique-fields [agg]))))

(defn union*
  "Merges the supplied flows and ensures uniqueness of the resulting
  tuples."
  [& flows]
  (-> (apply merge* flows)
      (unique)))

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
  (let [sink (to-sink sink)]
    (-> flow
        (in-branch (.getIdentifier sink)
                   (fn [subflow name]
                     (-> subflow
                         (update-in [:tails] conj (:pipe subflow))
                         (update-in [:sink-map] assoc name sink)))))))

(defn trap*
  "Applies a trap to the current branch of the supplied flow."
  [flow trap]
  (let [trap (to-sink trap)
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
