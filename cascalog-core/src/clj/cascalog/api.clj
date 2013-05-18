(ns cascalog.api
  (:use [cascalog.debug :only (debug-print)]
        [jackknife.core :only (safe-assert throw-runtime)]
        [jackknife.def :only (defalias)]
        [jackknife.seq :only (unweave collectify)])
  (:require [clojure.set :as set]
            [cascalog.vars :as v]
            [cascalog.predicate :as p]
            [cascalog.rules :as rules]
            [cascalog.util :as u]
            [cascalog.fluent.tap :as tap]
            [cascalog.fluent.conf :as conf]
            [cascalog.fluent.workflow :as w]
            [cascalog.fluent.flow :as flow]
            [cascalog.fluent.def :as d]
            [cascalog.fluent.operations :as ops]
            [cascalog.fluent.io :as io]
            [cascalog.fluent.cascading :refer (generic-cascading-fields?)]
            [hadoop-util.core :as hadoop])
  (:import [cascading.flow Flow FlowDef]
           [cascading.flow.hadoop HadoopFlowConnector]
           [cascading.tuple Fields]
           [cascading.pipe Pipe]
           [com.twitter.maple.tap StdoutTap]))

;; Functions for creating taps and tap helpers

(defalias memory-source-tap w/memory-source-tap)
(defalias cascalog-tap tap/mk-cascalog-tap)
(defalias hfs-tap tap/hfs-tap)
(defalias lfs-tap tap/lfs-tap)
(defalias sequence-file w/sequence-file)
(defalias text-line w/text-line)
(defalias hfs-textline tap/hfs-textline)
(defalias lfs-textline tap/lfs-textline)
(defalias hfs-seqfile tap/lfs-seqfile)
(defalias lfs-seqfile tap/lfs-seqfile)
(defalias stdout tap/stdout)

;; ## Query introspection

(defmulti get-out-fields
  "Get the fields of a generator."
  rules/generator-selector)

(defmethod get-out-fields :tap [tap]
  (let [cfields (.getSourceFields tap)]
    (safe-assert (not (generic-cascading-fields? cfields))
                 (str "Cannot get specific out-fields from tap. Tap source fields: "
                      cfields))
    (vec (seq cfields))))

(defmethod get-out-fields :generator [query]
  (:outfields query))

(defmethod get-out-fields :cascalog-tap [cascalog-tap]
  (get-out-fields (:source cascalog-tap)))

(defmethod get-out-fields :java-subquery [sq]
  (get-out-fields (.getCompiledSubquery sq)))

(defn num-out-fields [gen]
  (if (or (list? gen) (vector? gen))
    (count (first gen))
    ;; TODO: should pluck from Tap if it doesn't define out-fields
    (count (get-out-fields gen))))

;; ## Knobs for Hadoop

(defalias with-job-conf conf/with-job-conf)
(defalias with-serializations conf/with-serializations)

;; ## Query creation and execution

(defn construct
  "Construct a query or predicate macro functionally. When
constructing queries this way, operations should either be vars for
operations or values defined using one of Cascalog's def macros. Vars
must be stringified when passed to construct. If you're using
destructuring in a predicate macro, the & symbol must be stringified
as well."
  [outvars preds]
  (rules/build-rule (v/sanitize outvars)
                    (map rules/mk-raw-predicate preds)))

(defmacro <-
  "Constructs a query or predicate macro from a list of
  predicates. Predicate macros support destructuring of the input and
  output variables."
  [outvars & predicates]
  `(v/with-logic-vars
     (construct ~outvars [~@(map vec predicates)])))

(def cross-join
  (<- [:>] (identity 1 :> _)))

(defn compile-flow
  "Attaches output taps to some number of subqueries and creates a
  Cascading flow. The flow can be executed with `.complete`, or
  introspection can be done on the flow.

  Syntax: (compile-flow sink1 query1 sink2 query2 ...)
  or (compile-flow flow-name sink1 query1 sink2 query2)

   If the first argument is a string, that will be used as the name
  for the query and will show up in the JobTracker UI."
  [& args]
  (let [[flow-name bindings] (flow/parse-exec-args args)
        [sinks gens] (->> bindings
                          (map rules/normalize-gen)
                          (partition 2)
                          (mapcat (partial apply rules/normalize-sink-connection))
                          (unweave))
        gens      (map rules/enforce-gen-schema gens)
        sourcemap (apply merge (map :sourcemap gens))
        trapmap   (apply merge (map :trapmap gens))
        tails     (map rules/connect-to-sink gens sinks)
        sinkmap   (w/taps-map tails sinks)]
    (flow/run! (doto (FlowDef.)
                 (.setName flow-name)
                 (.addSources sourcemap)
                 (.addSinks sinkmap)
                 (.addTraps trapmap)
                 (.addTails (into-array Pipe tails))))))

(defn explain
  "Explains a query (by outputting a DOT file).

  outfile  - String location for DOT file output.
  sink-tap - Sink tap for query. Shows on query explanation. Defaults to stdout if omitted.
  query    - Query to be explained.

  Syntax: (explain outfile query)  or (explain outfile sink query)

  Ex: (explain \"outfile.dot\" (<- [?a ?b] ([[1 2]] ?a ?b)))
  "
  ([^String outfile query]
     (explain outfile (stdout) query))
  ([^String outfile sink-tap query]
     (let [^Flow flow (compile-flow sink-tap query)]
       (.writeDOT flow outfile))))

(defn ?-
  "Executes 1 or more queries and emits the results of each query to
  the associated tap.

  Syntax: (?- sink1 query1 sink2 query2 ...)  or (?- query-name sink1
  query1 sink2 query2)

   If the first argument is a string, that will be used as the name
  for the query and will show up in the JobTracker UI."
  [& bindings]
  (let [^Flow flow (apply compile-flow bindings)]
    (.complete flow)
    (when-not (-> flow .getFlowStats .isSuccessful)
      (throw-runtime "Flow failed to complete."))))

(defn ??-
  "Executes one or more queries and returns a seq of seqs of tuples
   back, one for each subquery given.

  Syntax: (??- query1 query2 ...) or (??- query-name query1 query2 ...)

  If the first argument is a string, that will be used as the name
  for the query and will show up in the JobTracker UI."
  [& args]
  (let [[name [& subqueries]] (flow/parse-exec-args args)]
    (io/with-fs-tmp [fs tmp]
      (hadoop/mkdirs fs tmp)
      (let [outtaps (for [q subqueries] (hfs-seqfile (str tmp "/" (u/uuid))))
            bindings (mapcat vector outtaps subqueries)]
        (apply ?- name bindings)
        (doall (map tap/get-sink-tuples outtaps))))))

(defmacro ?<-
  "Helper that both defines and executes a query in a single call.

  Syntax: (?<- out-tap out-vars & predicates) or (?<- \"myflow\"
  out-tap out-vars & predicates) ; flow name must be a static string
  within the ?<- form."
  [& args]
  ;; This is the best we can do... if want non-static name should just
  ;; use ?-
  (let [[name [output & body]] (flow/parse-exec-args args)]
    `(?- ~name ~output (<- ~@body))))

(defmacro ??<-
  "Like ??-, but for ?<-. Returns a seq of tuples."
  [& args]
  `(first (??- (<- ~@args))))

(defn predmacro*
  "Functional version of predmacro. See predmacro for details."
  [pred-macro-fn]
  (p/predicate p/predicate-macro
               (fn [invars outvars]
                 (for [[op & vars] (pred-macro-fn invars outvars)]
                   [op vars]))))

(defmacro predmacro
  "A more general but more verbose way to create predicate macros.

   Creates a function that takes in [invars outvars] and returns a
   list of predicates. When making predicate macros this way, you must
   create intermediate variables with gen-nullable-var(s). This is
   because unlike the (<- [?a :> ?b] ...) way of doing pred macros,
   Cascalog doesn't have a declaration for the inputs/outputs.

   See https://github.com/nathanmarz/cascalog/wiki/Predicate-macros
  "
  [& body]
  `(predmacro* (fn ~@body)))

(defn union
  "Merge the tuples from the subqueries together into a single
  subquery and ensure uniqueness of tuples."
  [& gens]
  (rules/combine* gens true))

(defn combine
  "Merge the tuples from the subqueries together into a single
  subquery. Doesn't ensure uniqueness of tuples."
  [& gens]
  (rules/combine* gens false))

(defn multigroup*
  [declared-group-vars buffer-out-vars buffer-spec & sqs]
  (let [[buffer-op hof-args]
        (if (sequential? buffer-spec) buffer-spec [buffer-spec nil])
        sq-out-vars (map get-out-fields sqs)
        group-vars (apply set/intersection (map set sq-out-vars))
        num-vars (reduce + (map count sq-out-vars))
        pipes (w/pipes-array (map :pipe sqs))
        args [declared-group-vars :fn> buffer-out-vars]
        args (if hof-args (cons hof-args args) args)]
    (safe-assert (seq declared-group-vars)
                 "Cannot do global grouping with multigroup")
    (safe-assert (= (set group-vars)
                    (set declared-group-vars))
                 "Declared group vars must be same as intersection of vars of all subqueries")
    (p/predicate p/generator nil
                 true
                 (apply merge (map :sourcemap sqs))
                 ((apply buffer-op args) pipes num-vars)
                 (concat declared-group-vars buffer-out-vars)
                 (apply merge (map :trapmap sqs)))))

(defmacro multigroup
  [group-vars out-vars buffer-spec & sqs]
  `(multigroup* ~(v/sanitize group-vars)
                ~(v/sanitize out-vars)
                ~buffer-spec
                ~@sqs))

;; TODO: All of these are actually just calling through to "select*"
;; in the fluent API.

(defmulti select-fields
  "Select fields of a named generator.

  Example:
  (<- [?a ?b ?sum]
      (+ ?a ?b :> ?sum)
      ((select-fields generator [\"?a\" \"?b\"]) ?a ?b))"
  rules/generator-selector)

(defmethod select-fields :tap [tap fields]
  (let [fields (collectify fields)
        pname  (u/uuid)
        outfields (v/gen-nullable-vars (count fields))
        pipe (w/assemble (w/pipe pname)
                         (w/identity fields :fn> outfields :> outfields))]
    (p/predicate p/generator nil true {pname tap} pipe outfields {})))

(defmethod select-fields :generator [query select-fields]
  (let [select-fields (collectify select-fields)
        outfields     (:outfields query)]
    (safe-assert (set/subset? (set select-fields)
                              (set outfields))
                 (format "Cannot select % from %."
                         select-fields
                         outfields))
    (merge query
           {:pipe (w/assemble (:pipe query) (w/select select-fields))
            :outfields select-fields})))

(defmethod select-fields :cascalog-tap [cascalog-tap fields]
  (select-fields (:source cascalog-tap)
                 fields))

(defmethod select-fields :java-subquery [sq fields]
  (select-fields (.getCompiledSubquery sq)
                 fields))

;; TODO: This should only call rename* on the underlying pipe.

(defn name-vars [gen vars]
  (let [vars (collectify vars)]
    (<- vars
        (gen :>> vars)
        (:distinct false))))

;; ## Defining custom operations

(defalias defmapop d/defmapfn
  "Defines a custom operation that appends new fields to the input tuple.")

(defalias defmapcatop d/defmapcatfn)
(defalias defbufferop d/defbufferfn)
;; (defalias defmultibufferop w/defmultibufferop)
(defalias defbufferiterop d/defbufferiterfn)
(defalias defaggregateop d/defaggregatefn)
(defalias deffilterop d/deffilterfn)
(defalias defparallelagg p/defparallelagg)
(defalias defparallelbuf p/defparallelbuf)

;; ## Miscellaneous helpers

(defn div
  "Perform floating point division on the arguments. Use this instead
   of / in Cascalog queries since / produces Ratio types which aren't
   serializable by Hadoop."
  [f & rest]
  (apply / (double f) rest))

(defmacro with-debug
  "Wrap queries in this macro to cause debug information for the query
   planner to be printed out."
  [& body]
  `(binding [cascalog.debug/*DEBUG* true]
     ~@body))

;; ## Class Creation

(defmacro defmain
  "Defines an AOT-compiled function with the supplied
  `name`. Containing namespace must be marked for AOT compilation to
  have any effect."
  [name & forms]
  (let [classname (namespace-munge (str *ns* "." name))
        sym (with-meta
              (symbol (str name "-main"))
              (meta name))]
    `(do (gen-class :name ~classname
                    :main true
                    :prefix ~(str name "-"))
         (defn ~(u/meta-conj sym {:no-doc true
                                  :skip-wiki true})
           ~@forms)
         (defn ~name ~@forms))))
