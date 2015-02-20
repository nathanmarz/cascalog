(ns cascalog.cascading.util
  (:require [clojure.set :refer (subset?)]
            [hadoop-util.core :as hadoop]
            [jackknife.core :refer (uuid)]
            [jackknife.seq :refer (collectify)]
            [cascalog.cascading.conf :as conf]
            [cascalog.logic.fn :as serfn])
  (:import [cascalog Util]
           [cascading.tap Tap]
           [cascading.pipe Pipe]
           [cascading.operation BaseOperation Function OperationCall FunctionCall
            ConcreteCall Filter Aggregator]
           [cascading.tuple Fields Tuple TupleEntry TupleEntryCollector]
           [cascading.flow FlowProcess]
           [cascading.flow.hadoop.util HadoopUtil]
           [clojure.lang IPersistentCollection]))

(defn pipe
  "Returns a Pipe of the given name, or if one is not supplied with a
   unique random name."
  ([] (pipe (uuid)))
  ([^String name]
     (Pipe. name)))

(defn ^Fields fields
  "Returns the supplied object as an instance of Cascading Fields."
  [x]
  (cond (instance? Fields x) x
        (nil? x) Fields/NONE
        :else (let [coll (collectify x)]
                (if (empty? coll)
                  Fields/NONE
                  (->> coll
                       (map str)
                       (into-array String)
                       (Fields.))))))

(defn fields-array
  [fields-seq]
  (into-array Fields (map fields fields-seq)))

(defn pipes-array
  [pipes]
  (into-array Pipe pipes))

(defn taps-array
  [taps]
  (into-array Tap taps))

(defn generic-cascading-fields?
  [^Fields cfields]
  (or (.isSubstitution cfields)
      (.isUnknown cfields)
      (.isArguments cfields)
      (.isResults cfields)
      (.isSwap cfields)
      (.isReplace cfields)))

(defn default-output
  "Returns a default output field vector for operations that accept
  from-fields and return to-fields."
  [^Fields from-fields ^Fields to-fields]
  (let [from (into #{} from-fields)
        to   (into #{} to-fields)]
    (cond
     ;; If the op returns Fields/ARGS, replace the input.
     (.isArguments to-fields) Fields/REPLACE

     ;; zero size is Fields/ALL, Fields/UNKNOWN, etc
     (zero? (.size from-fields)) Fields/ALL

     ;; If the fields are equal, replace the input with output.
     (and (= from to)) Fields/REPLACE

     ;; The output is a superset and replaces the input, OR the output
     ;; is a subset and throws away some input.
     (or (subset? from to) (subset? to from)) Fields/SWAP

     ;; Either the sets are disjoint (in which case everything is
     ;; fine) or the intersection is strange and cascading will throw.
     :else Fields/ALL)))

;; ## Operations
;;
;; Map and MapCat operations in pure clojure.

(comment
  ;; Java vs Clojure version:
  (def java-m
    (cascalog.ClojureMap. (fields "num") inc))
  (def clj-m
    (cascalog-map inc (fields "num")))

  ;; TODO: If we ever use these, not that we'll need to wrap the
  ;; dynamic stats variables in cascalog.cascading.stats.
  )

(defn collect-to
  [^TupleEntryCollector collector v]
  (let [^Tuple t (if (instance? java.util.List v)
                   (Tuple. (to-array v))
                   (doto (Tuple.) (.add v)))]
    (.add collector t)))

(defn cascalog-map
  [op-var ^Fields output-fields]
  (let [ser (serfn/serialize op-var)]
    (proxy [BaseOperation Function] [output-fields]
      (prepare [^FlowProcess flow-process ^OperationCall op-call]
        (let [op (serfn/deserialize ser)]
          (.setContext op-call op)))
      (operate [^FlowProcess flow-process ^FunctionCall fn-call]
        (let [op (.getContext fn-call)
              collector (.getOutputCollector fn-call)
              ^Tuple args (-> fn-call .getArguments .getTuple)
              res (apply op args)]
        (collect-to collector res)))
      (cleanup [flow-process ^OperationCall op-call]))))

(defn cascalog-mapcat
  [op-var ^Fields output-fields]
  (let [ser (serfn/serialize op-var)]
    (proxy [BaseOperation Function] [output-fields]
      (prepare [^FlowProcess flow-process ^OperationCall op-call]
        (let [op (serfn/deserialize ser)]
          (.setContext op-call op)))
      (operate [^FlowProcess flow-process ^FunctionCall fn-call]
        (let [op (.getContext fn-call)
              collector (.getOutputCollector fn-call)
              ^Tuple args (-> fn-call .getArguments .getTuple)]
          (doseq [r (apply op args)]
            (collect-to collector r))))
      (cleanup [flow-process ^OperationCall op-call]))))

(defn roundtrip
  "Round trip the supplied object through Cascading's base64
  serialization."
  [obj]
  (let [conf (hadoop/job-conf (conf/project-conf))]
    (-> (HadoopUtil/serializeBase64 obj conf true)
        (HadoopUtil/deserializeBase64 conf (class obj) true))))

(defn- output-collector [out-atom]
  (proxy [TupleEntryCollector] []
    (add [^Tuple tuple]
      (swap! out-atom conj (Util/coerceFromTuple tuple)))))

(defn- mk-op-call []
  (let [args-atom    (atom nil)
        out-atom     (atom [])
        context-atom (atom nil)]
    (proxy [ConcreteCall IPersistentCollection] []
      (setArguments [tuple]
        (swap! args-atom (constantly tuple)))
      (getArguments []
        @args-atom)
      (getOutputCollector []
        (output-collector out-atom))
      (setContext [context]
        (swap! context-atom (constantly context)))
      (getContext []
        @context-atom)
      (seq []
        (seq @out-atom)))))

(defn bench-op
  "Invokes the supplied Cascading Function."
  [m coll]
  (let [^Function m (roundtrip m)
        ^ConcreteCall func-call (mk-op-call)
        fp-null FlowProcess/NULL]
    (.setArguments func-call (TupleEntry. (Util/coerceToTuple coll)))
    (.prepare m fp-null func-call)
    (time (dotimes [_ 1000000]
            (.operate m fp-null func-call)))
    (.cleanup m fp-null func-call)))

(defn invoke-filter
  "Invokes the supplied Cascading filter with some tuple. The filter
  will return true or false."
  [fil coll]
  (let [^Filter fil (roundtrip fil)
        op-call (ConcreteCall.)
        fp-null FlowProcess/NULL]
    (.setArguments op-call (TupleEntry. (Util/coerceToTuple coll)))
    (.prepare fil fp-null op-call)
    (let [rem (.isRemove fil fp-null op-call)]
      (.cleanup fil fp-null op-call)
      rem)))

(defn invoke-function
  "Invokes the supplied Cascading Function."
  [m coll]
  (let [^Function m (roundtrip m)
        ^ConcreteCall func-call (mk-op-call)
        fp-null FlowProcess/NULL]
    (.setArguments func-call (TupleEntry. (Util/coerceToTuple coll)))
    (.prepare m fp-null func-call)
    (.operate m fp-null func-call)
    (.cleanup m fp-null func-call)
    (seq func-call)))

(defn invoke-aggregator [a colls]
  (let [^Aggregator a (roundtrip a)
        ^ConcreteCall ag-call (mk-op-call)
        fp-null FlowProcess/NULL]
    (.prepare a fp-null ag-call)
    (.start a fp-null ag-call)
    (doseq [coll colls]
      (.setArguments ag-call (TupleEntry. (Util/coerceToTuple coll)))
      (.aggregate a fp-null ag-call))
    (.complete a fp-null ag-call)
    (.cleanup  a fp-null ag-call)
    (seq ag-call)))
