(ns cascalog.cascading.operations
  (:require [clojure.tools.macro :refer (name-with-attributes)]
            [clojure.set :as set :refer (subset? difference intersection)]
            [cascalog.logic.fn :as serfn]
            [cascalog.logic.vars :as v]
            [cascalog.logic.algebra :refer (sum)]
            [cascalog.logic.platform :refer (generator)]
            [cascalog.cascading.util :as casc :refer (fields default-output)]
            [cascalog.cascading.tap :as tap]
            [cascalog.cascading.types :refer (to-sink)]
            [jackknife.core :refer (safe-assert throw-illegal uuid)]
            [jackknife.seq :as s :refer (unweave collectify)])
  (:import [cascading.tuple Fields]
           [cascalog.ops KryoInsert]
           [cascading.operation Identity Debug NoOp]
           [cascading.operation.filter Sample FilterNull]
           [cascading.operation.aggregator First Count Sum Min Max]
           [cascading.pipe Pipe Each Every GroupBy CoGroup Merge HashJoin Checkpoint]
           [cascading.pipe.joiner Joiner InnerJoin LeftJoin RightJoin OuterJoin]
           [cascading.pipe.joiner CascalogJoiner CascalogJoiner$JoinType]
           [cascading.pipe.assembly Rename AggregateBy]
           [cascalog ClojureFilter ClojureMapcat ClojureMap
            ClojureBuffer ClojureBufferIter FastFirst
            MultiGroupBy ClojureMultibuffer]
           [cascalog.aggregator ClojureAggregator
            ClojureMonoidAggregator ClojureMonoidFunctor
            ClojureAggregateBy CombinerSpec]))

;; ## Operations
;;
;; All of these operations work on implementers of the Generator
;; protocol, defined in cascalog.cascading.types.

(defmacro assembly [args & ops]
  `(fn [flow# ~@args]
     (-> flow# ~@ops)))

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

(defn name-flow
  "Assigns a new name to the clojure flow."
  [gen name]
  (-> (generator gen)
      (assoc :name name)))

(defn rename-pipe
  ([gen] (rename-pipe gen (uuid)))
  ([gen name]
     (add-op gen #(Pipe. name %))))

(defop select*
  "Remove all but the supplied fields from the given flow."
  [keep-fields]
  #(Each. %
          (fields keep-fields)
          (Identity. (fields keep-fields))))

(defn identity*
  "Mirrors the supplied set of input fields into the output fields."
  [flow input output]
  (each flow #(Identity. %) input output))

(defop discard*
  "Discard the supplied fields."
  [drop-fields]
  #(Each. % (fields drop-fields) (NoOp.) Fields/SWAP))

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
          Fields/NONE
          out-fields)))

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
  "Creates a parallel aggregation operation."
  [agg-fn in-fields out-fields & {:keys [init-var present-var]}]
  (let [in-fields (fields in-fields)
        out-fields (fields out-fields)]
    (reify
      IAggregateBy
      (aggregate-by [_]
        (let [map-spec (-> (CombinerSpec. agg-fn)
                           (.setPrepareFn init-var))
              reduce-spec (-> (CombinerSpec. agg-fn)
                              (.setPresentFn present-var))]
          (ClojureAggregateBy. in-fields
                               (ClojureMonoidFunctor. out-fields map-spec)
                               (ClojureMonoidAggregator. out-fields reduce-spec))))
      IAggregator
      (add-aggregator [_ pipe]
        (let [spec (-> (CombinerSpec. agg-fn)
                       (.setPrepareFn init-var)
                       (.setPresentFn present-var))]
          (Every. pipe in-fields
                  (ClojureMonoidAggregator. out-fields spec)))))))

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
          (throw-illegal
           "Cannot use both aggregators and buffers in the same grouping.")
          ::buffer)

        (and (not force-reduce?)
             (every? parallel-agg? aggregators))
        ::parallel

        :else ::aggregate))

(defn- groupby
  "Adds a raw GroupBy operation to the pipe. Don't use this directly."
  [name pipe group-fields sort-fields reverse?]
  (if (seq name)
    (if sort-fields
      (GroupBy. name pipe group-fields
                (fields sort-fields)
                (boolean reverse?))
      (GroupBy. name pipe group-fields))
    (if sort-fields
      (GroupBy. pipe group-fields
                (fields sort-fields)
                (boolean reverse?))
      (GroupBy. pipe group-fields))))

(defn- aggby [name pipe group-fields spill-threshold aggs]
  (let [aggs (->> aggs
                  (map aggregate-by)
                  (into-array AggregateBy))]
    (if (seq name)
      (AggregateBy. name pipe (fields group-fields) spill-threshold aggs)
      (AggregateBy. pipe (fields group-fields) spill-threshold aggs))))

;; TODO: Add proper assertions around sorting. (We can't sort when
;; we're in AggregateBy, for example.
;;
;; Note that sorting fields will force a reduce step.

(defop group-by*
  "Applies a grouping operation to the supplied generator."
  [group-fields aggs
   & {:keys [reducers spill-threshold sort-fields reverse? reduce-only name]
      :or {spill-threshold 0}}]
  (fn [pipe]
    (let [group-fields (fields group-fields)
          build-group  (fn [thunk]
                         (thunk
                          (groupby name pipe group-fields
                                   (fields sort-fields)
                                   reverse?)))
          mode (aggregate-mode aggs (or reduce-only sort-fields))]
      (case mode
        ::buffer    (build-group #(add-buffer (first aggs) %))
        ::aggregate (build-group (fn [grouped]
                                   (reduce (fn [p op]
                                             (add-aggregator op p))
                                           grouped aggs)))
        ::parallel  (aggby name pipe group-fields spill-threshold aggs)
        (throw-illegal "Unsupported aggregation mode: " mode)))))

(defn unique-aggregator []
  (reify IAggregator
    (add-aggregator [_ pipe]
      (Every. pipe (FastFirst.) Fields/RESULTS))))

(defn unique
  "Performs a unique on the input pipe by the supplied fields."
  ([flow] (unique flow Fields/ALL))
  ([flow unique-fields & options]
     (apply group-by* flow unique-fields [(unique-aggregator)] options)))

(defn union*
  "Merges the supplied flows and ensures uniqueness of the resulting
  tuples."
  [& flows]
  (unique (sum flows)))

;; ## Join Operations

(defn join->joiner
  "Converts the supplier joiner instance or keyword to a Cascading
  Joiner."
  [join]
  (if (instance? Joiner join)
    join
    (case join
      :inner (InnerJoin.)
      :outer (OuterJoin.)
      (throw-illegal "Can't create joiner from " join))))

(defmacro build-join-group
  [group-op group-name pipes group-fields decl-fields join]
  `(let [group-fields# (into-array Fields (map fields ~group-fields))
         joiner#       (join->joiner ~join)
         d#            ~decl-fields
         decl-fields#  (when d# (fields d#))]
     (if (seq ~group-name)
       (~group-op ~group-name ~pipes group-fields# decl-fields# joiner#)
       (~group-op ~pipes group-fields# decl-fields# joiner#))))

(defn- add-co-group-aggs
  [pipe aggs]
  (let [mode (aggregate-mode aggs true)]
    (case mode
      ::buffer (add-buffer (first aggs) pipe)
      ::aggregate (reduce (fn [p op]
                            (add-aggregator op p)) pipe aggs))))

(defn lift-pipes [flows]
  (map #(add-op % (fn [p] (into-array Pipe [p]))) flows))

(defn- ensure-unique-pipes
  [flows]
  (map rename-pipe flows))

(defn co-group*
  [flows group-fields & {:keys [decl-fields aggs reducers join name] :or {join :inner}}]
  (-> flows
      ensure-unique-pipes
      lift-pipes
      sum
      (add-op (fn [pipes]
                (-> (build-join-group CoGroup. name pipes group-fields decl-fields join)
                    (set-reducers reducers)
                    (add-co-group-aggs (or aggs [])))))))

(defn join-with-smaller
  [larger-flow fields1 smaller-flow fields2 & opts]
  (apply co-group*
         [larger-flow smaller-flow]
         [fields1 fields2]
         (concat opts [:join (InnerJoin.)])))

(defn join-with-larger
  [smaller-flow fields1 larger-flow fields2 group-fields aggs & opts]
  (apply join-with-smaller larger-flow fields2 smaller-flow fields1 opts))

(defn left-join-with-smaller
  [larger-flow fields1 smaller-flow fields2 aggs & opts]
  (apply co-group*
         [larger-flow smaller-flow]
         [fields1 fields2]
         (concat opts [:join (LeftJoin.)])))

(defn left-join-with-larger
  [smaller-flow fields1 larger-flow fields2 aggs & {:as opts}]
  (apply co-group*
         [larger-flow smaller-flow]
         [fields2 fields1]
         (concat opts [:join (RightJoin.)])))

(defn- cascalog-joiner-type
  [join]
  (case join
      :inner CascalogJoiner$JoinType/INNER
      :outer CascalogJoiner$JoinType/OUTER
      :exists CascalogJoiner$JoinType/EXISTS))

(defn join-many
  "Takes a sequence of [pipe, join-fields, join-type] triplets along
   with other co-group arguments and performs a mixed join. Allowed
   join types are :inner, :outer, and :exists."
  [flow-joins decl-fields options]
  (let [[flows group-fields join-types] (apply map vector flow-joins)
        join-types (map cascalog-joiner-type join-types)]
    (apply co-group*
           flows
           group-fields
           (concat options [:decl-fields decl-fields :join (CascalogJoiner. join-types)]))))


(defn hash-join*
  "Performs a map-side join of flows on join-fields. By default
   does an inner join, but callers can specify a join type using
   :join keyword argument, which can be :inner, :outer, or a
   Cascading Joiner implementation.

   Note: full or right outer joins have odd behavior in hash joins.
         See Cascading documentation for details."
  [flows join-fields & {:keys [join decl-fields name] :or {join :inner}}]
  (safe-assert (= (count flows) (count join-fields))
               "Expected same number of flows and join fields")
  (-> flows
      ensure-unique-pipes
      lift-pipes
      sum
      (add-op (fn [pipes]
                (build-join-group HashJoin. name pipes join-fields decl-fields join)))))

(defn hash-join-with-tiny
  [larger-flow fields1 tiny-flow fields2]
  (hash-join* [larger-flow tiny-flow]
              [fields1 fields2]))

(defn left-hash-join-with-tiny
  [larger-flow fields1 tiny-flow fields2]
  (hash-join* [larger-flow tiny-flow]
              [fields1 fields2]
              :join (LeftJoin.)))

(defn hash-join-many
  "Takes a sequence of [pipe, join-fields, join-type] triplets along
   with other hash-join arguments and performs a mixed join. Allowed
   join types are :inner, :outer, and :exists. The first entry must
   be of join type :inner."
  [flow-joins decl-fields]
  (safe-assert (= :inner (-> flow-joins (first) (nth 2)))
               "First (left-most) flow must be inner joined.")
  (let [[flows group-fields join-types] (apply map vector flow-joins)
        join-types (map cascalog-joiner-type join-types)]
    (hash-join* flows
                group-fields
                :decl-fields decl-fields
                :join (CascalogJoiner. join-types))))


(defn generate-join-fields [numfields numpipes]
  (repeatedly numpipes (partial v/gen-nullable-vars numfields)))

(defn replace-join-fields [join-fields join-renames fields]
  (let [replace-map (zipmap join-fields join-renames)]
    (reduce (fn [ret f]
              (let [newf (-> (replace-map f) (or f))]
                (conj ret newf)))
            [] fields)))

(defn declared-fields
  "Accepts a sequence of join fields and a sequence of
  field-seqs (each containing the join-fields, presumably) and returns
  a full vector of unique field names, suitable for the return value
  of a co-group."
  [join-fields renames infields]
  (flatten (map (partial replace-join-fields join-fields)
                renames
                infields)))

(defn join-fields-selector
  "Returns a selector that's used to go pull out groups from the join
  that aren't all nil."
  [num-fields]
  (serfn/fn [& args]
    (let [joins (partition num-fields args)]
      (or (s/find-first (partial s/some? (complement nil?)) joins)
          (repeat num-fields nil)))))

(defn new-pipe-name [joined-seq]
  (.getName (:pipe (:gen (first joined-seq)))))

(defrecord Inner [gen available-fields])
(defrecord Outer [gen available-fields])
(defrecord Existence [gen available-fields out-field])

(defn fields-to-keep
  "We want to keep the out-field of Existence nodes and all available
  fields of the Inner and Outer nodes."
  [gen-seq]
  (let [grouped (group-by type gen-seq)]
    (vec (set
          (concat (mapcat :available-fields (grouped Inner))
                  (mapcat :available-fields (grouped Outer))
                  (map :out-field (grouped Existence)))))))

(defn ensure-project
  "Makes sure that the declared fields are in the proper order."
  [gen-seq]
  (let [grouped (group-by type gen-seq)]
    (->> (concat (grouped Inner)
                 (grouped Outer)
                 (grouped Existence))
         (map (fn [g]
                (update-in g [:gen] #(-> %
                                         (select* (:available-fields g))
                                         (rename-pipe))))))))

(defn build-triplet
  [gen join-fields]
  [(:gen gen) join-fields (condp instance? gen
                            Inner :inner
                            Outer :outer
                            Existence :exists)])
(defn cascalog-join
  [gen-seq join-fields options]
  (let [final-name (new-pipe-name gen-seq)
        gen-seq (ensure-project gen-seq)
        in-fields (map :available-fields gen-seq)
        join-size (count join-fields)
        renames  (generate-join-fields join-size (count gen-seq))
        declared (declared-fields join-fields renames in-fields)
        to-keep (fields-to-keep gen-seq)
        select-exists (fn [joined]
                        (->> (mapcat (fn [g join-renames]
                                       (if (instance? Existence g)
                                         [[(first join-renames) (:out-field g)]]))
                                     gen-seq renames)
                             (reduce (fn [flow [in out]]
                                       (-> flow (identity* in out)))
                                     joined)))]
    (-> (join-many (map #(build-triplet % join-fields) gen-seq)
                   declared
                   options)
        (select-exists)
        (map* (join-fields-selector join-size)
              (flatten renames)
              join-fields)
        (select* to-keep)
        (rename-pipe final-name))))

;; ## MultiGroup

(defn multigroup
  "Take a sequence of pairs of [pipe, join-fields]"
  [pairs declared-group-vars op out-fields]
  (safe-assert (seq declared-group-vars)
               "Cannot do global grouping with multigroup")
  (let [flows (map (comp generator first) pairs)
        out-vars (map second pairs)
        group-vars (apply set/intersection (map set out-vars))
        num-vars (reduce + (map count out-vars))]
    (safe-assert (= (set group-vars)
                    (set declared-group-vars))
                 "Declared group vars must be same as intersection of
                 vars of all subqueries")
    (-> flows lift-pipes sum
        (add-op (fn [pipes]
                  (MultiGroupBy. pipes
                                 (casc/fields declared-group-vars)
                                 num-vars
                                 (ClojureMultibuffer. (casc/fields out-fields)
                                                      op)))))))

;; ## Output Operations
;;
;; This section covers output and traps

(defn lazy-generator
  "Returns a cascalog generator on the supplied sequence of
  tuples. `lazy-generator` serializes each item in the lazy sequence
  into a sequencefile located at the supplied temporary directory and returns
  a tap for the data in that directory.

  It's recommended to wrap queries that use this tap with
  `cascalog.cascading.io/with-fs-tmp`; for example,

    (with-fs-tmp [_ tmp-dir]
      (let [lazy-tap (lazy-generator tmp-dir lazy-seq)]
        (?<- (stdout)
             [?field1 ?field2 ... etc]
             (lazy-tap ?field1 ?field2)
             ...)))"
  [tmp-path [tuple :as l-seq]]
  {:pre [(coll? tuple)]}
  (let [tap (:sink (tap/hfs-seqfile tmp-path))
        n-fields (count tuple)]
    (tap/fill-tap! tap l-seq)
    (rename* tap (v/gen-nullable-vars n-fields))))

(defn in-branch
  "Accepts a temporary name and a function from flow => flow and
  performs the operation within a renamed branch."
  ([flow f]
     (in-branch flow (uuid) f))
  ([flow name f]
     (-> flow
         (rename-pipe name)
         (f name)
         (rename-pipe))))

(defn write* [flow sink]
  (let [sink (to-sink sink)]
    (-> flow
        (in-branch (uuid)
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

(defn checkpoint*
  "Forces results of the flow so far to be written to temp file, ensuring
   a M/R job boundary at this point in the flow.

  TODO: allow specifying a tap for checkpoint"
  [flow]
  (add-op flow #(Checkpoint. %)))

;; TODO: Figure out if I really understand what's going on with the
;; trap options. Do this by testing the traps with a few throws inside
;; and one after. Make sure the throw after causes a failure, but not
;; inside.
;;

(defn with-trap*
  "Applies a trap to everything that occurs within the supplied
  function of flow => flow."
  [flow trap f]
  (-> flow (trap* trap) f (rename-pipe)))

;; ## Logic Variable Substitution Rules

(defn with-duplicate-inputs
  "Accepts a flow, some fields, and a function from (flow,
  unique-fields, new-fields) => flow and appropriately handles
  duplicate entries inside of the fields.

  The fields passed to the supplied function will be guaranteed
  unique. New fields are passed as a third option to the supplying
  function, which may decide to call (discard* delta) if the fields
  are still around."
  [flow from-fields f]
  (if (or (empty? from-fields)
          (apply distinct? (collectify from-fields)))
    (f flow from-fields [])
    (let [[delta cleaned-fields] (v/replace-dups from-fields)]
      (-> (reduce (fn [subflow [field gen]]
                    (if (= field gen)
                      subflow
                      (identity* subflow field gen)))
                  flow
                  (map vector from-fields cleaned-fields))
          (f cleaned-fields (seq delta))))))

;; TODO: If we have some sort of ignored variable coming out of a
;; Cascalog query, we want to strip all operations out at that
;; point. Probably when we're building up a generator.

(defn substitute-if
  "Returns [newseq {map of newvals to oldvals}]"
  [pred subfn aseq]
  (reduce (fn [[newseq subs] val]
            (let [[newval sub] (if (pred val)
                                 (let [subbed (subfn val)] [subbed {subbed val}])
                                 [val {}])]
              [(conj newseq newval) (merge subs sub)]))
          [[] {}] aseq))

(defn constant-substitutions
  "Returns a 2-vector of the form

   [new variables, {map of newvars to values to substitute}]"
  [vars]
  (substitute-if (complement v/cascalog-var?)
                 (fn [_] (v/gen-nullable-var))
                 (collectify vars)))

(defn insert-subs [flow sub-m]
  (if (empty? sub-m)
    flow
    (apply insert* flow (apply concat sub-m))))

(defn with-constants
  "Allows constant substitution on inputs."
  [gen in-fields f]
  (let [[new-input sub-m] (constant-substitutions in-fields)
        ignored (keys sub-m)
        gen (-> (insert-subs gen sub-m)
                (f new-input))]
    (if (seq ignored)
      (discard* gen (fields ignored))
      gen)))

(defn- replace-ignored-vars
  "Replaces all ignored variables with a nullable cascalog
  variable. "
  [vars]
  (map (fn [v] (if (= "_" v) (v/gen-nullable-var) v))
       (collectify vars)))

(defn filter-nullable-vars
  "If there are any nullable variables present in the output, filter
  nulls out now."
  [flow all-fields]
  (if-let [non-null-fields (seq (filter v/non-nullable-var? all-fields))]
    (add-op flow #(Each. % (fields non-null-fields) (FilterNull.)))
    flow))

(defn no-overlap? [large small]
  (empty?
   (intersection (set (collectify large))
                 (set (collectify small)))))

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
        (with-duplicate-inputs gen in
          (fn [gen in delta]
            (let [gen (-> gen
                          (f (fields in)
                             (fields new-output)))
                  gen (if-let [to-discard (not-empty
                                           (fields (concat delta ignored)))]
                        (discard* gen to-discard)
                        gen)]
              (filter-nullable-vars gen new-output))))))))

(comment
  "This one works, since all inputs are logical variables."
  (-> [1 2 3 4]
      (rename* "?a")
      (map* inc "?a" "?other")
      (logically ["?a" 10 "?other"] "?b"
                 (fn [gen in out]
                   (-> gen (map* * in out))))
      (cascalog.cascading.flow/to-memory))

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
  composition).")
