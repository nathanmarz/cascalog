(ns cascalog.cascading.flow
  (:refer-clojure :exclude [run!])
  (:require [jackknife.core :as u]
            [jackknife.seq :refer (unweave)]
            [hadoop-util.core :as hadoop]
            [cascalog.cascading.conf :as conf]
            [cascalog.cascading.types :as types]
            [cascalog.cascading.tap :as tap]
            [cascalog.cascading.stats :as stats]
            [cascalog.cascading.io :as io]
            [cascalog.cascading.operations :as ops]
            [cascalog.logic.algebra :refer (sum)]
            [cascalog.logic.parse :refer (parse-exec-args)]
            [schema.core :as s])
  (:import [cascalog Util]
           [cascading.pipe Pipe Merge]
           [cascading.tap Tap]
           [cascading.flow FlowDef]
           [cascalog.cascading.types ClojureFlow]
           [cascading.flow.hadoop HadoopFlow HadoopFlowConnector]))

;; ## Stats

(def ^:dynamic *stats-fn* nil)

(defmacro with-stats
  "Executes the supplied body with the supplied stats fn."
  [f & body]
  `(binding [*stats-fn* ~f]
     ~@body))

(s/defn handle-stats! :- stats/StatsMap
  "If the stats function is bound, passes the supplied stats map in
  and returns it unchanged."
  [sm :- stats/StatsMap]
  (when-let [f *stats-fn*]
    (f sm))
  sm)

;; ## Flow Building

(s/defn flow-def :- FlowDef
  "Generates an instance of FlowDef off of the supplied ClojureFlow."
  [{:keys [source-map sink-map trap-map tails name]} :- ClojureFlow]
  (doto (FlowDef.)
    (.setName name)
    (.addSources source-map)
    (.addSinks sink-map)
    (.addTraps trap-map)
    (.addTails (into-array Pipe tails))))

(s/defn compile-hadoop :- HadoopFlow
  "Compiles the supplied FlowDef into a Hadoop flow."
  [fd :- FlowDef]
  (-> (HadoopFlowConnector.
       (conf/project-merge (conf/project-conf)
                           {"cascading.flow.job.pollinginterval" 10}))
      (.connect fd)))

(s/defn graph :- ClojureFlow
  "Writes a dotfile for the flow at hand to the supplied path."
  [flow :- ClojureFlow path :- s/Str]
  (-> (flow-def flow)
      compile-hadoop
      (.writeDOT path))
  flow)

;; TODO: Add support for supplying a name to the flow-def at this
;; stage. Not sure if we're going to be able to apply the name to the
;; HadoopFlow.

(s/defn assert-success! [sm :- stats/StatsMap]
  (or (:successful? sm)
      (throw (ex-info "Flow failed to complete." sm))))

(defprotocol IRunnable
  "All runnable items should implement this function."
  (run! [x] "Runs the flow and returns an instance of
  cascalog.cascading.stats/StatsMap."))

(extend-protocol IRunnable
  HadoopFlow
  (run! [flow]
    (.complete flow)
    (handle-stats!
     (stats/stats-map (.getFlowStats flow))))

  FlowDef
  (run! [fd]
    (run! (compile-hadoop fd)))

  ClojureFlow
  (run! [flow]
    (run! (flow-def flow))))

(s/defn compile-flow :- ClojureFlow
  "Attaches output taps to some number of subqueries and creates a
  Cascading flow. The flow can be executed with `.complete`, or
  introspection can be done on the flow.

  Syntax: (compile-flow sink1 query1 sink2 query2 ...)
  or (compile-flow flow-name sink1 query1 sink2 query2)

   If the first argument is a string, that will be used as the name
  for the query and will show up in the JobTracker UI."
  [& args]
  (let [strip-pipe (fn [m] (assoc m :pipe nil))
        [name bindings] (parse-exec-args args)
        [sinks gens] (unweave bindings)]
    (cond-> (sum (map (comp strip-pipe ops/write*)
                      gens sinks))
            (not-empty name) (ops/name-flow name))))

(s/defn jflow-def :- FlowDef
  [& args]
  (let [flow (apply compile-flow args)]
    (flow-def flow)))

(s/defn jcompile-flow :- HadoopFlow
  [& args]
  (compile-hadoop (apply jflow-def args)))

(defn all-to-memory
  "Return the results of the supplied workflows as data
  structures. Accepts many workflows, and (optionally) a flow name as
  the first argument."
  [& args]
  (let [[name flows] (parse-exec-args args)]
    (io/with-fs-tmp [fs tmp]
      (hadoop/mkdirs fs tmp)
      (let [taps (->> (u/unique-rooted-paths tmp)
                      (map tap/hfs-seqfile)
                      (take (count flows)))]
        (assert-success!
         (run! (apply compile-flow name (interleave taps flows))))
        (doall (map tap/get-sink-tuples taps))))))

(defn to-memory
  "Executes the supplied flow and returns the results as a sequence of
  tuples."
  [m]
  (first (all-to-memory m)))
