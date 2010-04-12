(ns cascalog.api
  (:use [cascalog vars util graph])
  (:require cascalog.rules)
  (:require [cascalog [workflow :as w] [predicate :as p]])
  (:import [cascading.flow Flow FlowConnector])
  (:import [cascading.tuple Fields])
  (:import [cascalog StdoutTap])
  (:import  [cascading.pipe Pipe]))

(def DEFAULT-OPTIONS
  {:distinct true})

(defmacro <-
  "Constructs a rule from a list of predicates"
  [& args]
  (let [[farg & rargs] args
        [options outvars predicates] (if (map? farg)
                                      [(merge DEFAULT-OPTIONS farg) (first rargs) (rest rargs)]
                                      [DEFAULT-OPTIONS farg rargs])
        predicate-builders (vec (map cascalog.rules/mk-raw-predicate predicates))
        outvars-str (vars2str outvars)]
        `(cascalog.rules/build-rule ~options ~outvars-str ~predicate-builders)))

;; TODO: add ability to specify sorting of output (should this be specified in query or in <- options?)
(defn ?-
  "Builds and executes a flow based on the sinks binded to the rules. 
  Bindings are of form: sink rule"
  [& bindings]
  (let [[sinks gens]    (unweave bindings)
        sourcemap       (apply merge (map :sourcemap gens))
        tails           (map cascalog.rules/connect-to-sink gens sinks)
        sinkmap         (w/taps-map tails sinks)
        flow            (.connect (FlowConnector. {"cascading.flow.job.pollinginterval" 100})
                          sourcemap sinkmap (into-array Pipe tails))]
        (.complete flow)))

(defmacro ?<- [output & body]
  `(?- ~output (<- ~@body)))

(defn stdout [] (StdoutTap.))

(defn hfs-textline [path]
  (w/hfs-tap (w/text-line ["line"] Fields/ALL) path))

(defn lfs-textline [path]
  (w/hfs-tap (w/text-line ["line"] Fields/ALL) path))

(defn hfs-seqfile [path]
  (w/hfs-tap (w/sequence-file Fields/ALL) path))

(defn lfs-seqfile [path]
  (w/hfs-tap (w/sequence-file Fields/ALL) path))