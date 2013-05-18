(ns cascalog.fluent.flow
  (:require [cascalog.util :as u]
            [hadoop-util.core :as hadoop]
            [cascalog.fluent.conf :as conf]
            [cascalog.fluent.types :as types]
            [cascalog.fluent.tap :as tap]
            [cascalog.fluent.io :as io]
            [cascalog.fluent.operations :as ops])
  (:import [cascalog Util]
           [cascading.pipe Pipe Merge]
           [cascading.tap Tap]
           [cascading.flow FlowDef]
           [cascalog.fluent.types ClojureFlow]
           [cascading.flow.hadoop HadoopFlow HadoopFlowConnector]))

;; ## Flow Building

(defn flow-def
  "Generates an instance of FlowDef off of the supplied ClojureFlow."
  [{:keys [source-map sink-map trap-map tails]}]
  (doto (FlowDef.)
    (.addSources source-map)
    (.addSinks sink-map)
    (.addTraps trap-map)
    (.addTails (into-array Pipe tails))))

(defn compile-hadoop
  "Compiles the supplied FlowDef into a Hadoop flow."
  [fd]
  (-> (HadoopFlowConnector.
       (u/project-merge (conf/project-conf)
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

(defn parse-exec-args
  "Accept a sequence of (maybe) string and other items and returns a
  vector of [theString or \"\", [other items]]."
  [[f & rest :as args]]
  (if (string? f)
    [f rest]
    ["" args]))

(defn all-to-memory
  "Return the results of the supplied workflows as data
  structures. Accepts many workflows, and (optionally) a flow name as
  the first argument."
  [& args]
  (let [strip-pipe   (fn [m] (assoc m :pipe nil))
        [name flows] (parse-exec-args args)]
    (io/with-fs-tmp [fs tmp]
      (hadoop/mkdirs fs tmp)
      (let [taps (->> (u/unique-rooted-paths tmp)
                      (map tap/hfs-seqfile)
                      (take (count flows)))]
        (->> (map (comp strip-pipe ops/write*)
                  flows taps)
             (apply ops/merge*)
             (run!))
        (doall (map tap/get-sink-tuples taps))))))

(defn to-memory
  "Executes the supplied flow and returns the results as a sequence of
  tuples."
  [m]
  (first (all-to-memory m)))
