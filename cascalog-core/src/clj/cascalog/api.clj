(ns cascalog.api
  (:use [jackknife.seq :only (collectify)])
  (:require [clojure.set :as set]
            [cascalog.logic.def :as d]
            [cascalog.logic.algebra :as algebra]
            [cascalog.logic.vars :as v]
            [cascalog.logic.predicate :as p]
            [cascalog.logic.platform :as platform]
            [cascalog.logic.parse :as parse]
            [cascalog.logic.predmacro :as pm]
            [cascalog.cascading.platform :refer (compile-query)]
            [cascalog.cascading.tap :as tap]
            [cascalog.cascading.conf :as conf]
            [cascalog.cascading.flow :as flow]
            [cascalog.cascading.operations :as ops]
            [cascalog.cascading.tap :as tap]
            [cascalog.cascading.types :as types]
            [cascalog.cascading.io :as io]
            [cascalog.cascading.util :refer (generic-cascading-fields?)]
            [hadoop-util.core :as hadoop]
            [jackknife.core :as u]
            [jackknife.def :as jd :refer (defalias)])
  (:import [cascading.flow Flow]
           [cascading.tap Tap]
           [cascalog.logic.parse TailStruct]
           [cascalog.cascading.tap CascalogTap]
           [jcascalog Subquery]))

;; Functions for creating taps and tap helpers

(defalias memory-source-tap tap/memory-source-tap)
(defalias cascalog-tap tap/->CascalogTap)
(defalias hfs-tap tap/hfs-tap)
(defalias lfs-tap tap/lfs-tap)
(defalias sequence-file tap/sequence-file)
(defalias text-line tap/text-line)
(defalias hfs-textline tap/hfs-textline)
(defalias lfs-textline tap/lfs-textline)
(defalias hfs-seqfile tap/lfs-seqfile)
(defalias lfs-seqfile tap/lfs-seqfile)
(defalias stdout tap/stdout)

;; ## Query introspection

(defprotocol IOutputFields
  (get-out-fields [_] "Get the fields of a generator."))

(extend-protocol IOutputFields
  Tap
  (get-out-fields [tap]
    (let [cfields (.getSourceFields tap)]
      (u/safe-assert
       (not (generic-cascading-fields? cfields))
       (str "Cannot get specific out-fields from tap. Tap source fields: "
            cfields))
      (vec (seq cfields))))

  TailStruct
  (get-out-fields [tail]
    (:available-fields tail))

  Subquery
  (get-out-fields [sq]
    (get-out-fields (.getCompiledSubquery sq)))

  CascalogTap
  (get-out-fields [tap]
    (get-out-fields (:source tap))))

(defprotocol INumOutFields
  (num-out-fields [_]))

;; TODO: num-out-fields should try and pluck from Tap if it doesn't
;; define output fields, rather than just throwing immediately.

(extend-protocol INumOutFields
  Subquery
  (num-out-fields [sq]
    (count (seq (.getOutputFields sq))))

  CascalogTap
  (num-out-fields [tap]
    (num-out-fields (:source tap)))

  clojure.lang.ISeq
  (num-out-fields [x]
    (count (collectify (first x))))

  clojure.lang.IPersistentVector
  (num-out-fields [x]
    (count (collectify (peek x))))

  Tap
  (num-out-fields [x]
    (count (get-out-fields x)))

  TailStruct
  (num-out-fields [x]
    (count (:available-fields x))))

;; ## Knobs for Hadoop

(defalias with-job-conf conf/with-job-conf)
(defalias with-serializations conf/with-serializations)

;; ## Query creation and execution

(defalias construct parse/parse-subquery)

(defalias <- parse/<-)

(def cross-join
  (<- [:>] (identity 1 :> _)))

(defalias compile-flow flow/compile-flow)

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
       (flow/graph flow outfile))))

(defn normalize-sink-connection [sink subquery]
  (cond (fn? sink) (sink subquery)
        (instance? CascalogTap sink)
        (normalize-sink-connection (:sink sink) subquery)
        :else [sink subquery]))

(defn ?-
  "Executes 1 or more queries and emits the results of each query to
  the associated tap.

  Syntax: (?- sink1 query1 sink2 query2 ...)  or (?- query-name sink1
  query1 sink2 query2)

   If the first argument is a string, that will be used as the name
  for the query and will show up in the JobTracker UI."
  [& bindings]
  (let [[name bindings] (flow/parse-exec-args bindings)
        bindings (mapcat (partial apply normalize-sink-connection)
                         (partition 2 bindings))]
    (flow/run! (apply compile-flow name bindings))))

(defn ??-
  "Executes one or more queries and returns a seq of seqs of tuples
   back, one for each subquery given.

  Syntax: (??- query1 query2 ...) or (??- query-name query1 query2 ...)

  If the first argument is a string, that will be used as the name
  for the query and will show up in the JobTracker UI."
  [& args]
  (let [[name args] (flow/parse-exec-args args)]
    (apply flow/all-to-memory name (map compile-query args))))

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

(defalias predmacro* pm/predmacro*)
(defalias predmacro pm/predmacro)

(defn name-vars [gen vars]
  (let [vars (collectify vars)]
    (<- vars
        (gen :>> vars)
        (:distinct false))))

;; TODO: Handle cases where don't have ALL of the same
;; type. (combining a tap with a query, for example. This is the
;; normalization step.) group by type, get them to generators, then
;; combine each generator.

;; TODO: Turn EVERYONE into a tailstruct, then merge.

(defn to-tail [g & {:keys [fields]}]
  (cond (parse/tail? g) g
        (types/generator? g)
        (if (and (coll? g) (empty? g))
          (u/throw-illegal
           "Data structure is empty -- memory sources must contain tuples.")
          (let [names (or fields (v/gen-nullable-vars (num-out-fields g)))
                gen (types/generator g)]
            (name-vars gen names)))
        :else (u/throw-illegal "Can't combine " g)))

(defn combine
  "Merge the tuples from the subqueries together into a single
  subquery. Doesn't ensure uniqueness of tuples."
  [& [g & gens]]
  (let [g (to-tail g)
        names (get-out-fields g)
        gens (cons g (map #(to-tail % :fields names) gens))]
    (types/generator
     (algebra/sum gens))))

(defn union
  "Merge the tuples from the subqueries together into a single
  subquery and ensure uniqueness of tuples."
  [& gens]
  (ops/unique (apply combine gens)))

(defprotocol ISelectFields
  (select-fields [gen fields]
    "Select fields of a named generator.

  Example:
  (<- [?a ?b ?sum]
      (+ ?a ?b :> ?sum)
      ((select-fields generator [\"?a\" \"?b\"]) ?a ?b))"))

(extend-protocol ISelectFields
  TailStruct
  (select-fields [sq fields]
    (parse/project sq fields))

  Subquery
  (select-fields [sq fields]
    (select-fields (.getCompiledSubquery sq) fields))

  Tap
  (select-fields [tap fields]
    (-> (types/generator tap)
        (ops/select* fields))))

;; ## Defining custom operations

(defalias prepfn d/prepfn)
(defalias defprepfn d/defprepfn)

;; These functions allow you to lift a vanilla function up into
;; Cascalog. For example, (mapcatop vector) is identical to

(defalias mapop d/mapop)
(defalias filterop d/filterop)
(defalias mapcatop d/mapcatop)
(defalias bufferop d/bufferop)
(defalias aggregateop d/aggregateop)
(defalias bufferiterop d/bufferiterop)
(defalias parallelagg d/parallelagg)

(defalias mapfn d/mapfn)
(defalias filterfn d/filterfn)
(defalias mapcatfn d/mapcatfn)
(defalias bufferfn d/bufferfn)
(defalias bufferiterfn d/bufferiterfn)
(defalias aggregatefn d/aggregatefn)

(defalias defmapfn d/defmapfn)
(defalias deffilterfn d/deffilterfn)
(defalias defmapcatfn d/defmapcatfn)
(defalias defbufferfn d/defbufferfn)
(defalias defbufferiterfn d/defbufferiterfn)
(defalias defaggregatefn d/defaggregatefn)

(defalias defmapop d/defmapop)
(defalias deffilterop d/deffilterop)
(defalias defmapcatop d/defmapcatop)
(defalias defbufferop d/defbufferop)
(defalias defbufferiterop d/defbufferiterop)
(defalias defaggregateop d/defaggregateop)

(defalias defparallelagg d/defparallelagg)

;; ## Miscellaneous helpers

(defn div
  "Perform floating point division on the arguments. Use this instead
   of / in Cascalog queries since / produces Ratio types which aren't
   serializable by Hadoop."
  [f & rest]
  (apply / (double f) rest))

;; ## Class Creation

(defalias defmain jd/defmain)
