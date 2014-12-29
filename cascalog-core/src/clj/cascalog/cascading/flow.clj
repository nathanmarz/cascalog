(ns cascalog.cascading.flow
  (:require [jackknife.core :as u]
            [jackknife.seq :refer (unweave)]
            [hadoop-util.core :as hadoop]
            [cascalog.cascading.conf :as conf]
            [cascalog.cascading.types :as types]
            [cascalog.cascading.tap :as tap]
            [cascalog.cascading.io :as io]
            [cascalog.cascading.operations :as ops]
            [cascalog.logic.algebra :refer (sum)]
            [cascalog.logic.parse :refer (parse-exec-args)])
  (:import [cascalog Util]
           [cascading.pipe Pipe Merge]
           [cascading.tap Tap]
           [cascading.flow FlowDef]
           [cascalog.cascading.types ClojureFlow]
           [cascading.flow.hadoop HadoopFlow HadoopFlowConnector]))

;; ## Flow Building

(defn flow-def
  "Generates an instance of FlowDef off of the supplied ClojureFlow."
  [{:keys [source-map sink-map trap-map tails name]}]
  (doto (FlowDef.)
    (.setName name)
    (.addSources source-map)
    (.addSinks sink-map)
    (.addTraps trap-map)
    (.addTails (into-array Pipe tails))))

(defn compile-hadoop
  "Compiles the supplied FlowDef into a Hadoop flow."
  [fd]
  (-> (HadoopFlowConnector.
       (conf/project-merge (conf/project-conf)
                           {"cascading.flow.job.pollinginterval" 10}))
      (.connect fd)))

(defn graph
  "Writes a dotfile for the flow at hand to the supplied path."
  [flow path]
  (-> (flow-def flow)
      compile-hadoop
      (.writeDOT path))
  flow)

;; TODO: Add support for supplying a name to the flow-def at this
;; stage. Not sure if we're going to be able to apply the name to the
;; HadoopFlow.

(defprotocol IRunnable
  "All runnable items should implement this function."
  (run! [x]))

(extend-protocol IRunnable
  HadoopFlow
  (run! [flow]
    (.complete flow)
    (when-not (-> flow .getFlowStats .isSuccessful)
      (throw (RuntimeException. "Flow failed to complete."))))

  FlowDef
  (run! [fd]
    (run! (compile-hadoop fd)))

  ClojureFlow
  (run! [flow]
    (run! (flow-def flow))))

(defn compile-flow
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
    (-> (map (comp strip-pipe ops/write*)
             gens sinks)
        (sum)
        (ops/name-flow name))))

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
        (run! (apply compile-flow name (interleave taps flows)))
        (doall (map tap/get-sink-tuples taps))))))

(defn to-memory
  "Executes the supplied flow and returns the results as a sequence of
  tuples."
  [m]
  (first (all-to-memory m)))
