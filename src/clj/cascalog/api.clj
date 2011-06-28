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

(ns cascalog.api
  (:use [cascalog vars util graph debug])
  (:use [clojure.contrib def])
  (:require cascalog.rules)
  (:require [clojure [set :as set]])
  (:require [cascalog [workflow :as w] [predicate :as p] [io :as io] [hadoop :as hadoop]])
  (:import [cascading.flow Flow FlowConnector])
  (:import [cascading.tuple Fields])
  (:import [cascalog StdoutTap Util MemorySourceTap])
  (:import [cascading.pipe Pipe])
  (:import [java.util ArrayList]))


;; Functions for creating taps and tap helpers

(defalias memory-source-tap w/memory-source-tap)

(defn cascalog-tap
  "Defines a cascalog tap which can be used to add additional abstraction over cascading taps.
  
   'source' can be a cascading tap, subquery, or a cascalog tap.
   'sink' can be a cascading tap, sink function, or a cascalog tap."
  [source sink]
  (struct-map cascalog.rules/cascalog-tap :type :cascalog-tap :source source :sink sink))

(defnk hfs-textline
  "Creates a tap on HDFS using textline format. Different filesystems
   can be selected by using different prefixes for {path}. (Optional
   `sinkmode` argument can be one of `:keep`, `:include` or
   `:replace`.)
   
   See http://www.cascading.org/javadoc/cascading/tap/Hfs.html and
   http://www.cascading.org/javadoc/cascading/scheme/TextLine.html"
  [path :sinkmode nil]
  (w/hfs-tap (w/text-line ["line"] Fields/ALL)
             path
             :sinkmode sinkmode))

(defnk lfs-textline
  "Creates a tap on the local filesystem using textline
   format. (Optional `sinkmode` argument can be one of `:keep`,
   `:include` or `:replace`.)
   
   See http://www.cascading.org/javadoc/cascading/tap/Lfs.html and
   http://www.cascading.org/javadoc/cascading/scheme/TextLine.html"
  [path :sinkmode nil]
  (w/lfs-tap (w/text-line ["line"] Fields/ALL)
             path
             :sinkmode sinkmode))

(defnk hfs-seqfile
  "Creates a tap on HDFS using sequence file format. Different
   filesystems can be selected by using different prefixes for
   {path}. (Optional `sinkmode` argument can be one of `:keep`,
   `:include` or `:replace`.)
   
   See http://www.cascading.org/javadoc/cascading/tap/Hfs.html and
   http://www.cascading.org/javadoc/cascading/scheme/SequenceFile.html"
  [path :sinkmode nil]
  (w/hfs-tap (w/sequence-file Fields/ALL)
             path
             :sinkmode sinkmode))

(defnk lfs-seqfile
  "Creates a tap that reads data off of the local filesystem in
   sequence file format. (Optional `sinkmode` argument can be one of
   `:keep`, `:include` or `:replace`.)
   
   See http://www.cascading.org/javadoc/cascading/tap/Lfs.html and
   http://www.cascading.org/javadoc/cascading/scheme/SequenceFile.html"
  [path :sinkmode nil]
  (w/lfs-tap (w/sequence-file Fields/ALL)
             path
             :sinkmode sinkmode))

(defn stdout
  "Creates a tap that prints tuples sunk to it to standard
   output. Useful for experimentation in the REPL."
  [] (StdoutTap.))

;; Query introspection

(defmulti get-out-fields cascalog.rules/generator-selector)

(defmethod get-out-fields :tap [tap]
  (let [cfields (.getSourceFields tap)]
    (if (cascalog.rules/generic-cascading-fields? cfields)
      (throw (IllegalArgumentException. (str "Cannot get specific out-fields from tap. Tap source fields: " cfields)))
      (vec (seq cfields))
      )))

(defmethod get-out-fields :generator [query]
  (:outfields query))

(defmethod get-out-fields :cascalog-tap [cascalog-tap]
  (get-out-fields (:source cascalog-tap)))


;; Query creation and execution

(defmacro <-
  "Constructs a query or predicate macro from a list of
  predicates. Predicate macros support destructuring of the input and
  output variables."
  [outvars & predicates]
  (let [predicate-builders (vec (map cascalog.rules/mk-raw-predicate predicates))
        outvars-str (if (vector? outvars) (vars2str outvars) outvars)]
    `(cascalog.rules/build-rule ~outvars-str ~predicate-builders)))

(defn compile-flow
  "Attaches output taps to some number of subqueries and creates a
  Cascading flow. The flow can be executed with `.complete`, or
  introspection can be done on the flow.

  Syntax: (compile-flow sink1 query1 sink2 query2 ...)
  or (compile-flow flow-name sink1 query1 sink2 query2)
   
   If the first argument is a string, that will be used as the name
  for the query and will show up in the JobTracker UI."
  [& args]
  (let [[flow-name bindings] (cascalog.rules/parse-exec-args args)
        bindings     (mapcat (partial apply cascalog.rules/normalize-sink-connection)
                             (partition 2 bindings))
        [sinks gens] (unweave bindings)
        sourcemap    (apply merge (map :sourcemap gens))
        trapmap      (apply merge (map :trapmap gens))
        tails        (map cascalog.rules/connect-to-sink gens sinks)
        sinkmap      (w/taps-map tails sinks)]
    (.connect (->> cascalog.rules/*JOB-CONF*
                   (merge {"cascading.flow.job.pollinginterval" 100})
                   (FlowConnector.))
              flow-name
              sourcemap
              sinkmap
              trapmap
              (into-array Pipe tails))))

(defn ?-
  "Executes 1 or more queries and emits the results of each query to
  the associated tap.

  Syntax: (?- sink1 query1 sink2 query2 ...)  or (?- query-name sink1
  query1 sink2 query2)
   
   If the first argument is a string, that will be used as the name
  for the query and will show up in the JobTracker UI."
  [& bindings]
  (.complete (apply compile-flow bindings)))

(defn ??-
  "Executes one or more queries and returns a seq of seqs of tuples
   back, one for each subquery given.
  
  Syntax: (??- sink1 query1 sink2 query2 ...)"
  [& subqueries]
  ;; TODO: should be checking for flow name here
  (io/with-fs-tmp [fs tmp]
    (hadoop/mkdirs fs tmp)
    (let [outtaps (for [q subqueries] (hfs-seqfile (str tmp "/" (uuid))))
          bindings (mapcat vector outtaps subqueries)]
      (apply ?- bindings)
      (doall (map cascalog.rules/get-tuples outtaps))
      )))

(defmacro ?<-
  "Helper that both defines and executes a query in a single call.
  
  Syntax: (?<- out-tap out-vars & predicates) or (?<- \"myflow\"
  out-tap out-vars & predicates) ; flow name must be a static string
  within the ?<- form."
  [& args]
  ;; This is the best we can do... if want non-static name should just use ?-
  (let [[name [output & body]] (cascalog.rules/parse-exec-args args)]
    `(?- ~name ~output (<- ~@body))))

(defmacro ??<-
  "Like ??-, but for ?<-. Returns a seq of tuples."
  [& args]
  `(io/with-fs-tmp [fs# tmp1#]
     (let [outtap# (hfs-seqfile tmp1#)]
       (?<- outtap# ~@args)
       (cascalog.rules/get-tuples outtap#))
     ))

(defn predmacro*
  "Functional version of predmacro. See predmacro for details."
  [pred-macro-fn]
  (p/predicate p/predicate-macro
               (fn [invars outvars]
                 (for [[op & vars] (pred-macro-fn invars outvars)]
                   [op nil vars])
                 )))

(defmacro predmacro
  "A more general but more verbose way to create predicate macros.

   Creates a function that takes in [invars outvars] and returns a
   list of predicates. When making predicate macros this way, you must
   create intermediate variables with gen-nullable-var(s). This is
   because unlike the (<- [?a :> ?b] ...) way of doing pred macros,
   Cascalog doesn't have a declaration for the inputs/outputs."
  [& body]
  `(predmacro* (fn ~@body)))

(defn construct
  "Construct a query or predicate macro functionally. When
constructing queries this way, operations should either be vars for
operations or values defined using one of Cascalog's def macros. Vars
must be stringified when passed to construct. If you're using
destructuring in a predicate macro, the & symbol must be stringified
as well."
  [outvars preds]
  (let [outvars (vars2str outvars)
        preds (for [[p & vars] preds] [p nil (vars2str vars)])]
    (cascalog.rules/build-rule outvars preds)
    ))

(defn union
  "Merge the tuples from the subqueries together into a single
  subquery and ensure uniqueness of tuples."
  [& gens]
  (cascalog.rules/combine* gens true))

(defn combine
  "Merge the tuples from the subqueries together into a single
  subquery. Doesn't ensure uniqueness of tuples."
  [& gens]
  (cascalog.rules/combine* gens false))

(defn multigroup*
  [declared-group-vars buffer-out-vars buffer-spec & sqs]
  (let [[buffer-op hof-args] (if (sequential? buffer-spec) buffer-spec [buffer-spec nil])
        sq-out-vars (map get-out-fields sqs)
        group-vars (apply set/intersection (map set sq-out-vars))
        num-vars (reduce + (map count sq-out-vars))
        pipes (into-array Pipe (map :pipe sqs))
        args [declared-group-vars :fn> buffer-out-vars]
        args (if hof-args (cons hof-args args) args)]
    (when (empty? declared-group-vars)
      (throw (IllegalArgumentException. "Cannot do global grouping with multigroup")))
    (when-not (= (set group-vars) (set declared-group-vars))
      (throw (IllegalArgumentException. "Declared group vars must be same as intersection of vars of all subqueries")))
    (p/predicate p/generator nil
                 true
                 (apply merge (map :sourcemap sqs))
                 ((apply buffer-op args) pipes num-vars)
                 (concat declared-group-vars buffer-out-vars)
                 (apply merge (map :trapmap sqs))
                 )))

(defmacro multigroup
  [group-vars out-vars buffer-spec & sqs]
  `(multigroup* ~(vars2str group-vars)
                ~(vars2str out-vars)
                ~buffer-spec
                ~@sqs))

(defmulti select-fields cascalog.rules/generator-selector)

(defmethod select-fields :tap [tap fields]
  (let [fields (collectify fields)
        pname (uuid)
        outfields (gen-nullable-vars (count fields))
        pipe (w/assemble (w/pipe pname) (w/identity fields :fn> outfields :> outfields))]
    (p/predicate p/generator nil true {pname tap} pipe outfields {})))

(defmethod select-fields :generator [query select-fields]
  (let [select-fields (collectify select-fields)
        outfields (:outfields query)]
    (when-not (set/subset? (set select-fields) (set outfields))
      (throw (IllegalArgumentException. (str "Cannot select " select-fields " from " outfields))))
    (merge query
           {:pipe (w/assemble (:pipe query) (w/select select-fields))
            :outfields select-fields}
           )))

(defmethod select-fields :cascalog-tap [cascalog-tap select-fields]
  (select-fields (:source cascalog-tap) select-fields))

(defn name-vars [gen vars]
  (let [vars (collectify vars)]
    (<- vars (gen :>> vars) (:distinct false))
    ))


;; Defining custom operations

(defalias defmapop w/defmapop
  "Defines a custom operation that appends new fields to the input tuple.")

(defalias defmapcatop w/defmapcatop
  "")

(defalias defbufferop w/defbufferop)

(defalias defmultibufferop w/defmultibufferop)

(defalias defbufferiterop w/defbufferiterop)

(defalias defaggregateop w/defaggregateop)

(defalias deffilterop w/deffilterop)

(defalias defparallelagg p/defparallelagg)

(defalias defparallelbuf p/defparallelbuf)

;; Knobs for Hadoop

(defmacro with-job-conf
  "Modifies the job conf for queries executed within the form. Nested
   with-job-conf calls will merge configuration maps together, with
   innermost calls taking precedence on conflicting keys."
  [conf & body]
  `(binding [cascalog.rules/*JOB-CONF* (merge cascalog.rules/*JOB-CONF* ~conf)]
     ~@body))

;; Miscellaneous helpers

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
