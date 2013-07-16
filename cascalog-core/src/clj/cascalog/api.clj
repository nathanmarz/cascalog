(ns cascalog.api
  (:use [jackknife.core :only (safe-assert uuid)]
        [jackknife.seq :only (collectify)])
  (:require [clojure.set :as set]

            [cascalog.logic.def :as d]
            [cascalog.logic.algebra :as algebra]
            [cascalog.logic.vars :as v]
            [cascalog.logic.predicate :as p]
            [cascalog.logic.parse :as parse]
            [cascalog.logic.predmacro :as pm]
            [cascalog.cascading.platform :refer (compile-query)]
            [cascalog.cascading.tap :as tap]
            [cascalog.cascading.conf :as conf]
            [cascalog.cascading.flow :as flow]
            [cascalog.cascading.operations :as ops]
            [cascalog.cascading.tap :as tap]
            [cascalog.cascading.io :as io]
            [cascalog.cascading.util :refer (generic-cascading-fields?)]
            [hadoop-util.core :as hadoop]
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
      (safe-assert (not (generic-cascading-fields? cfields))
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
    (count (get-out-fields x))))

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
       (.writeDOT flow outfile))))

(defn ?-
  "Executes 1 or more queries and emits the results of each query to
  the associated tap.

  Syntax: (?- sink1 query1 sink2 query2 ...)  or (?- query-name sink1
  query1 sink2 query2)

   If the first argument is a string, that will be used as the name
  for the query and will show up in the JobTracker UI."
  [& bindings]
  (flow/run! (apply compile-flow bindings)))

(defn ??-
  "Executes one or more queries and returns a seq of seqs of tuples
   back, one for each subquery given.

  Syntax: (??- query1 query2 ...) or (??- query-name query1 query2 ...)

  If the first argument is a string, that will be used as the name
  for the query and will show up in the JobTracker UI."
  [& args]
  (apply flow/all-to-memory (map compile-query args)))

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

;; TODO: Obviously these are still a little busted. We need to go
;; ahead and rename everyone to the same shit.

(defn union
  "Merge the tuples from the subqueries together into a single
  subquery and ensure uniqueness of tuples."
  [& gens]
  (apply ops/union*
         (map cascalog.cascading.types/generator gens)))

(defn combine
  "Merge the tuples from the subqueries together into a single
  subquery. Doesn't ensure uniqueness of tuples."
  [& gens]
  (algebra/sum (map cascalog.cascading.types/generator gens)))

;; TODO: All of these are actually just calling through to "select*"
;; in the cascading API.

(defmulti select-fields
  "Select fields of a named generator.

  Example:
  (<- [?a ?b ?sum]
      (+ ?a ?b :> ?sum)
      ((select-fields generator [\"?a\" \"?b\"]) ?a ?b))"
  generator-selector)

(comment


  (defmethod select-fields :tap [tap fields]
    (let [fields (collectify fields)
          pname  (uuid)
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

  (defmethod select-fields :java-subquery [sq fields]
    (select-fields (.getCompiledSubquery sq)
                   fields))

  ;; TODO: This should only call rename* on the underlying pipe.

  (defn name-vars [gen vars]
    (let [vars (collectify vars)]
      (<- vars
          (gen :>> vars)
          (:distinct false)))))

;; ## Defining custom operations

(defalias defmapop d/defmapfn
  "Defines a custom operation that appends new fields to the input tuple.")

(defalias mapfn d/mapfn)
(defalias bufferfn d/bufferfn)
(defalias defmapcatop d/defmapcatfn)
(defalias defbufferop d/defbufferfn)
;; (defalias defmultibufferop w/defmultibufferop)
(defalias defbufferiterop d/defbufferiterfn)
(defalias defaggregateop d/defaggregatefn)
(defalias deffilterop d/deffilterfn)
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
