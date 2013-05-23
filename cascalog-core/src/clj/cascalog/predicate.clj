(ns cascalog.predicate
  (:use [cascalog.util :only (uuid multifn? substitute-if search-for-var any-list?)]
        [jackknife.seq :only (transpose)]
        [clojure.tools.macro :only (name-with-attributes)])
  (:require [jackknife.core :as u]
            [cascalog.vars :as v]
            [cascalog.fluent.workflow :as w]
            [cascalog.fluent.operations :as ops]
            [cascalog.fluent.types :as types])
  (:import [cascading.tap Tap]
           [cascading.operation Filter]
           [cascading.tuple Fields]
           [clojure.lang IFn]
           [jcascalog PredicateMacro Subquery ClojureOp PredicateMacroTemplate]
           [cascalog.aggregator CombinerSpec]
           [cascalog ClojureParallelAggregator ClojureBuffer
            ClojureBufferCombiner CascalogFunction
            CascalogFunctionExecutor CascadingFilterToFunction
            CascalogBuffer CascalogBufferExecutor CascalogAggregator
            CascalogAggregatorExecutor ClojureParallelAgg ParallelAgg]))

(defstruct parallel-aggregator
  :type
  :init-var
  :combine-var)

;; :num-intermediate-vars-fn takes as input infields, outfields
(defstruct parallel-buffer
  :type
  :hof?
  :init-hof-var
  :combine-hof-var
  :extract-hof-var
  :num-intermediate-vars-fn
  :buffer-hof-var)

(defmacro defparallelagg
  "Binds an efficient aggregator to the supplied symbol. A parallel
  aggregator processes each tuple through an initializer function,
  then combines the results each tuple's initialization until one
  result is achieved. `defparallelagg` accepts two keyword arguments:

  :init-var -- A var bound to a fn that accepts raw tuples and returns
  an intermediate result; #'one, for example.

  :combine-var -- a var bound to a fn that both accepts and returns
  intermediate results.

  For example,

  (defparallelagg sum
  :init-var #'identity
  :combine-var #'+)

  Used as

  (sum ?x :> ?y)"
  {:arglists '([name doc-string? attr-map? & {:keys [init-var combine-var]}])}
  [name & body]
  (let [[name body] (name-with-attributes name body)]
    `(def ~name
       (struct-map cascalog.predicate/parallel-aggregator
         :type ::parallel-aggregator ~@body))))

(defmacro defparallelbuf
  {:arglists '([name doc-string? attr-map? & {:keys [init-hof-var
                                                     combine-hof-var
                                                     extract-hof-var
                                                     num-intermediate-vars-fn
                                                     buffer-hof-var]}])}
  [name & body]
  (let [[name body] (name-with-attributes name body)]
    `(def ~name
       (struct-map cascalog.predicate/parallel-buffer
         :type ::parallel-buffer ~@body))))

;; ids are so they can be used in sets safely

(defmacro defpredicate [name & attrs]
  `(defstruct ~name :type :id ~@attrs))

(defmacro predicate [aname & attrs]
  `(struct ~aname ~(keyword (name aname)) (uuid) ~@attrs))

;; for map, mapcat, and filter
(defpredicate operation
  :assembly
  :infields
  :outfields
  :allow-on-genfilter?)

;; return a :post-assembly, a :parallel-agg, and a :serial-agg-assembly
(defpredicate aggregator
  :buffer?
  :parallel-agg
  :pregroup-assembly
  :serial-agg-assembly
  :post-assembly
  :infields
  :outfields)

;; automatically generates source pipes and attaches to sources
(defrecord Generator [join-set-var ])
(defpredicate generator
  :join-set-var
  :ground?
  :sourcemap
  :pipe
  :outfields
  :trapmap)

(defpredicate generator-filter
  :generator
  :outvar)

(defpredicate outconstant-equal)

(defpredicate predicate-macro :pred-fn)

(def distinct-aggregator
  (predicate aggregator false nil identity (w/fast-first) identity [] []))

(defn predicate-dispatcher [pred]
  (let [op  (:op pred)
        ret (cond (types/generator? op)             ::generator
                  (instance? Filter op)             ::cascading-filter
                  (instance? CascalogFunction op)   ::cascalog-function
                  (instance? CascalogBuffer op)     ::cascalog-buffer
                  (instance? CascalogAggregator op) ::cascalog-aggregator
                  (instance? ParallelAgg op)        ::java-parallel-agg

                  ;; This dispatches generator-filter, generator,
                  ;; aggregator, operation.
                  (map? op)                         (:type op)
                  (or (vector? op) (any-list? op))  ::data-structure
                  (:pred-type (meta op))            (:pred-type (meta op))
                  (instance? IFn op)                ::vanilla-function
                  :else (u/throw-illegal (str op " is an invalid predicate.")))]
    (if (= ret :bufferiter) :buffer ret)))

(defn predicate-macro? [p]
  (or (var? p)
      (instance? PredicateMacro p)
      (instance? PredicateMacroTemplate p)
      (instance? Subquery p)
      (instance? ClojureOp p)
      (and (map? p) (= :predicate-macro (:type p)))))

(defn- ground-fields? [outfields]
  (every? v/ground-var? outfields))

(defn- init-trap-map [options]
  (if-let [trap (:trap options)]
    (loop [tap (:tap trap)]
      (if (map? tap)
        (recur (:sink tap))
        {(:name trap) tap}))
    {}))

(defn- init-pipe-name [{:keys [trap]}]
  (or (:name trap)
      (uuid)))

(defn- simpleop-build-predicate
  [op infields outfields _]
  (predicate operation
             (op infields :fn> outfields :> Fields/ALL)
             infields
             outfields
             false))

(defn- simpleagg-build-predicate
  [buffer? op infields outfields _]
  (predicate aggregator
             buffer?
             nil
             identity
             (op infields :fn> outfields :> Fields/ALL)
             identity
             infields
             outfields))

(defmulti predicate-default-var predicate-dispatcher :default :>)
(defmethod predicate-default-var ::vanilla-function [& _] :<)
(defmethod predicate-default-var :filter [& _] :<)
(defmethod predicate-default-var ::cascading-filter [& _] :<)

(defmulti build-predicate-specific predicate-dispatcher)

;; The general logic is, create a name, attach a pipe to this bad boy
;; and build up a trap map.
(defmethod build-predicate-specific ::generator
  [{:keys [op output] :as pred}]
  (types/generator op)
  (let [sourcename (uuid) ;; Create a name,
        pname (init-pipe-name options)
        pipe (w/assemble (w/pipe sourcename)
                         (w/pipe-rename pname)
                         (w/identity Fields/ALL :fn> outfields :> Fields/RESULTS))]
    (u/safe-assert (empty? infields) "Cannot use :> in a taps vars declaration")
    (predicate generator
               nil
               (ground-fields? outfields)
               {sourcename tap}
               pipe
               outfields
               (init-trap-map options))))

(defmethod build-predicate-specific :generator
  [gen _ outfields options]
  (let [pname (init-pipe-name options)
        trapmap (merge (:trapmap gen)
                       (init-trap-map options))
        pipe (w/assemble (:pipe gen)
                         (w/pipe-rename pname)
                         (w/identity Fields/ALL :fn> outfields :> Fields/RESULTS))]
    (predicate generator
               nil
               (ground-fields? outfields)
               (:sourcemap gen)
               pipe
               outfields
               trapmap)))

(defmethod build-predicate-specific ::java-parallel-agg
  [java-pagg infields outfields _]
  (let [cascading-agg (ClojureParallelAggregator. (w/fields outfields)
                                                  java-pagg
                                                  (count infields))
        serial-assem (if (empty? infields)
                       (w/raw-every cascading-agg Fields/ALL)
                       (w/raw-every (w/fields infields)
                                    cascading-agg
                                    Fields/ALL))]
    (predicate aggregator
               false
               java-pagg
               identity
               serial-assem
               identity
               infields
               outfields)))

(defmethod build-predicate-specific ::parallel-aggregator
  [pagg infields outfields options]
  (let [init-spec (ops/fn-spec (:init-var pagg))
        combine-spec (ops/fn-spec (:combine-var pagg))
        java-pagg (ClojureParallelAgg. (-> (CombinerSpec. combine-spec)
                                           (.setPrepareFn init-spec)))]
    (build-predicate-specific java-pagg infields outfields options)
    ))

(letfn [(mk-hof-fn-spec [avar args]
          (ops/fn-spec (cons avar args)))]

  (defmethod build-predicate-specific ::parallel-buffer
    [pbuf infields outfields options]
    (let [temp-vars (v/gen-nullable-vars ((:num-intermediate-vars-fn pbuf)
                                          infields
                                          outfields))
          hof-args  {}
          sort-fields (:sort options)
          sort-fields (if (empty? sort-fields) nil sort-fields)
          combiner-spec (-> (CombinerSpec. (mk-hof-fn-spec (:combine-hof-var pbuf) hof-args))
                            (.setPrepareFn (mk-hof-fn-spec (:init-hof-var pbuf) hof-args))
                            (.setPresentFn (mk-hof-fn-spec (:extract-hof-var pbuf) hof-args)))
          combiner (fn [group-fields]
                     (w/raw-each Fields/ALL
                                 (ClojureBufferCombiner.
                                  (w/fields group-fields)
                                  (w/fields sort-fields)
                                  (w/fields infields)
                                  (w/fields temp-vars)
                                  combiner-spec)
                                 Fields/RESULTS))
          group-assembly (w/raw-every (w/fields temp-vars)
                                      (ClojureBuffer. (w/fields outfields)
                                                      (mk-hof-fn-spec (:buffer-hof-var pbuf) hof-args))
                                      Fields/ALL)]
      (predicate aggregator
                 true
                 combiner
                 identity group-assembly
                 identity
                 infields
                 outfields))))


(defmethod build-predicate-specific ::vanilla-function
  [afn infields outfields _]
  (let [opvar (search-for-var afn)
        _ (u/safe-assert opvar "Vanilla functions must have vars associated with them.")
        [func-fields out-selector] (if (not-empty outfields)
                                     [outfields Fields/ALL]
                                     [nil nil])
        assembly (w/filter opvar infields :fn> func-fields :> out-selector)]
    (predicate operation assembly infields outfields false)))

(defmethod build-predicate-specific :map [& args]
  (apply simpleop-build-predicate args))

(defmethod build-predicate-specific :mapcat [& args]
  (apply simpleop-build-predicate args))

(defmethod build-predicate-specific :aggregate [& args]
  (apply simpleagg-build-predicate false args))

(defmethod build-predicate-specific :buffer [& args]
  (apply simpleagg-build-predicate true args))

(defmethod build-predicate-specific :filter
  [op infields outfields _]
  (let [[func-fields out-selector] (if (not-empty outfields)
                                     [outfields Fields/ALL]
                                     [nil nil])
        assembly (op infields :fn> func-fields :> out-selector)]
    (predicate operation
               assembly
               infields
               outfields
               false)))

(defmethod build-predicate-specific ::cascalog-function
  [op infields outfields _]
  (predicate operation
             (w/raw-each (w/fields infields)
                         (CascalogFunctionExecutor. (w/fields outfields) op)
                         Fields/ALL)
             infields
             outfields
             false))

(defmethod build-predicate-specific ::cascading-filter
  [op infields outfields _]
  (u/safe-assert (#{0 1} (count outfields))
                 "Must emit 0 or 1 fields from filter")
  (let [c-infields (w/fields infields)
        assem (if (empty? outfields)
                (w/raw-each c-infields op)
                (w/raw-each c-infields
                            (CascadingFilterToFunction. (first outfields) op)
                            Fields/ALL))]
    (predicate operation assem infields outfields false)))

(defmethod build-predicate-specific ::cascalog-buffer
  [op infields outfields options]
  (predicate aggregator
             true
             nil
             identity
             (w/raw-every (w/fields infields)
                          (CascalogBufferExecutor. (w/fields outfields) op)
                          Fields/ALL)
             identity
             infields
             outfields))

(defmethod build-predicate-specific ::cascalog-aggregator
  [op infields outfields _]
  (predicate aggregator
             false
             nil
             identity
             (w/raw-every (w/fields infields)
                          (CascalogAggregatorExecutor. (w/fields outfields) op)
                          Fields/ALL)
             identity
             infields
             outfields))

(defmethod build-predicate-specific ::data-structure
  [tuples infields outfields options]
  (build-predicate-specific (w/memory-source-tap tuples)
                            infields
                            outfields
                            options))

(defmethod build-predicate-specific :generator-filter
  [op infields outfields options]
  (-> (build-predicate-specific (:generator op)
                                infields
                                outfields
                                options)
      (assoc :join-set-var (:outvar op))))

;; TODO: Document: what is this?
(defmethod build-predicate-specific :outconstant-equal
  [_ infields outfields options]
  (-> (build-predicate-specific = infields outfields options)
      (assoc :allow-on-genfilter? true)))

(defn- variable-substitution
  "Returns [newvars {map of newvars to values to substitute}]"
  [vars]
  (substitute-if (complement v/cascalog-var?)
                 (fn [_] (v/gen-nullable-var))
                 vars))

(w/deffilterop non-null? [& objs]
  (every? (complement nil?) objs))

(defn- mk-insertion-assembly [subs]
  (if (not-empty subs)
    (apply w/insert (transpose (seq subs)))
    identity))

(defn- replace-ignored-vars [vars]
  (map #(if (= "_" %) (v/gen-nullable-var) %) vars))

(defmulti enhance-predicate (fn [pred & _] (:type pred)))

(defmethod enhance-predicate :operation
  [pred infields inassem outfields outassem]
  (let [inassem  (or inassem  identity)
        outassem (or outassem identity)]
    (merge pred {:assembly (w/compose-straight-assemblies inassem
                                                          (:assembly pred)
                                                          outassem)
                 :outfields outfields
                 :infields infields})))

(defmethod enhance-predicate :aggregator
  [pred infields inassem outfields outassem]
  (let [inassem  (or inassem identity)
        outassem (or outassem identity)]
    (merge pred {:pregroup-assembly (w/compose-straight-assemblies
                                     inassem
                                     (:pregroup-assembly pred))
                 :post-assembly (w/compose-straight-assemblies
                                 (:post-assembly pred)
                                 outassem
                                 ;; work-around to cascading bug, TODO: remove when fixed in cascading
                                 (w/identity Fields/ALL :> Fields/RESULTS))
                 :outfields outfields
                 :infields infields})))

(defmethod enhance-predicate :generator
  [pred _ inassem outfields outassem]
  (when inassem
    (u/throw-runtime
     "Something went wrong in planner - generator received an input modifier"))
  (merge pred {:pipe (outassem (:pipe pred))
               :outfields outfields}))

;; TODO: Cascading won't allow multiple in-fields -- what we can do
;; instead, and it might be happening here, is add a new field,
;; perform the operation, then remove the field after the fact.

(defn- fix-duplicate-infields
  "Workaround to Cascading not allowing same field multiple times as
  input to an operation. Copies values as a workaround"
  [infields]
  (letfn [(update [[newfields dupvars assem] f]
            (if ((set newfields) f)
              (let [newfield (v/gen-nullable-var)
                    idassem (w/identity f :fn> newfield :> Fields/ALL)]
                [(conj newfields newfield)
                 (conj dupvars newfield)
                 (w/compose-straight-assemblies assem idassem)])
              [(conj newfields f) dupvars assem]))]
    (reduce update [[] [] identity] infields)))

(defn- mk-null-check [fields]
  (let [non-null-fields (filter v/non-nullable-var? fields)]
    (if (not-empty non-null-fields)
      (non-null? non-null-fields)
      identity)))

(defn build-predicate
  "Build a predicate. Calls down to build-predicate-specific for
  predicate-specific building and adds constant substitution and null
  checking of ? vars."
  [options op orig-infields outvars]
  (let [outvars                  (replace-ignored-vars outvars)
        [infields infield-subs]  (variable-substitution orig-infields)
        [infields dupvars duplicate-assem] (fix-duplicate-infields infields)
        predicate   (build-predicate-specific op infields outvars options)
        new-outvars (concat outvars (keys infield-subs) dupvars)
        in-insertion-assembly (when (seq infields)
                                (w/compose-straight-assemblies
                                 (mk-insertion-assembly infield-subs)
                                 duplicate-assem))
        null-check-out (mk-null-check outvars)]
    (enhance-predicate predicate
                       (filter v/cascalog-var? orig-infields)
                       in-insertion-assembly
                       new-outvars
                       null-check-out)))
