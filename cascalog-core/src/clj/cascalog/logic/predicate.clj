(ns cascalog.logic.predicate
  "TODO: We need to remove all of the Cascading implementations from
   here. The extensions to to-predicate."
  (:require [clojure.string :refer (join)]
            [jackknife.core :as u]
            [cascalog.logic.vars :as v]
            [cascalog.logic.def :as d]
            [cascalog.logic.fn :refer (search-for-var)]
            [cascalog.cascading.operations :as ops]
            [cascalog.cascading.types :as types])
  (:import [clojure.lang IFn]
           [cascalog.logic.def ParallelAggregator Prepared]
           [jcascalog Subquery ClojureOp]
           [cascalog CascalogFunction CascalogBuffer CascalogAggregator ParallelAgg]))

(defprotocol IRawPredicate
  (normalize [_]
    "Returns a sequence of RawPredicate instances."))

;; Raw Predicate type.

(defrecord RawPredicate [op input output]
  IRawPredicate
  (normalize [p] [p]))

;; Output of the subquery, the predicates it contains and the options
;; in the subquery.

(defrecord RawSubquery [fields predicates])

;; Printing Methods
;;
;; The following methods allow a predicate to print properly.

(defmethod print-method RawPredicate
  [{:keys [op input output]} ^java.io.Writer writer]
  (binding [*out* writer]
    (let [op (if (ifn? op)
               (let [op (or (::d/op (meta op)) op)]
                 (or (search-for-var op) op))
               op)]
      (print (str "(" op " "))
      (doseq [v (join " " input)]
        (print v))
      (when (not-empty output)
        (print " :> ")
        (doseq [v (join " " output)]
          (print v)))
      (println ")"))))

(defmethod print-method RawSubquery
  [{:keys [fields predicates]} ^java.io.Writer writer]
  (binding [*out* writer]
    (println "(<-" (vec fields))
    (doseq [pred predicates]
      (print "    ")
      (print-method pred writer))
    (println "    )")))

;; ## ICouldFilter

(defprotocol ICouldFilter
  "This protocol exists so that Cascalog can decide, if no input or
   output signifier exists, if the function takes inputs or outputs by
   default."
  (filter? [_]
    "Returns true if the object could filter, false otherwise."))

(extend-protocol ICouldFilter
  Object
  (filter? [_] false)

  clojure.lang.Fn
  (filter? [_] true)

  clojure.lang.MultiFn
  (filter? [_] true))

;; Leaves of the tree:
(defrecord Generator [gen fields])

;; GeneratorSets can't be unground, ever.
(defrecord GeneratorSet [generator join-set-var])

(defrecord Operation [op input output])

;; filters can be applied to Generator or GeneratorSet.
(defrecord FilterOperation [op input])

(defrecord Aggregator [op input output])

(defn validate-generator-set!
  "GeneratorSets can't be unground, ever."
  [input output]
  (when (not-empty input)
    (when (> (count output) 1)
      (u/throw-illegal "Only one output variable allowed in a generator-as-set."))
    (when-let [unground (not-empty (filter v/unground-var? (concat input output)))]
      (u/throw-illegal (str "Can't use unground vars in generators-as-sets. "
                            (vec unground)
                            " violate(s) the rules.\n\n")))))

;; TODO: This has horrendous duplication with the logically function
;; inside of operations.clj. The only reason for this duplication is
;; that I need to know what to name the fields initially -- I need to
;; know how to run a projection that doesn't clash with any of the
;; names.
;;
;;
;; I think that the way to fix this is to add a special
;; "generator-set" type, so that the parsing can properly resolve the
;; output fields. In the interest of getting the code out the door,
;; I'm going to push this like it is.
(defn sanitize-output
  "If the generator has duplicate output fields, this function
  generates duplicates and applies the proper equality operations."
  [gen output]
  (let [[output sub-m] (ops/constant-substitutions output)
        [_ cleaned] (ops/replace-dups output)
        gen (reduce (fn [acc [old new]]
                      (let [acc (if (= old new)
                                  acc
                                  (-> acc (ops/filter* = [old new])))]
                        acc
                        (if-let [const (sub-m new)]
                          (if (or (fn? const)
                                  (u/multifn? const))
                            (-> acc
                                (ops/filter* const [old new])
                                (ops/discard* new))
                            (let [new-v (v/uniquify-var new)]
                              (-> acc
                                  (ops/insert* new-v const)
                                  (ops/filter* = [new new-v])
                                  (ops/discard* new-v))))
                          acc)))
                    (-> (types/generator gen)
                        (ops/rename* cleaned)
                        (ops/filter-nullable-vars cleaned))
                    (map vector output cleaned))]
    [cleaned gen]))

(defn generator-node
  "Converts the supplied generator into the proper type of node."
  [gen input output]
  {:pre [(types/generator? gen)]}
  (if-not (empty? input)
    (do (validate-generator-set! input output)
        (-> (generator-node gen [] input)
            (->GeneratorSet (first output))))
    (let [[cleaned gen] (sanitize-output gen output)
          {:keys [pipe source-map trap-map]} gen]
      (->Generator gen cleaned))))

;; The following multimethod converts operations (in the first
;; position of a parsed cascalog predicate) to nodes in the graph.

(defmulti to-predicate
  (fn [op input output]
    (type op)))

(defmethod to-predicate :default
  [op _ _]
  (u/throw-illegal (str op " is an invalid predicate.")))

;; ## Operations

(defmethod to-predicate Subquery
  [op input output]
  (to-predicate (.getCompiledSubquery op) input output))

(defmethod to-predicate ClojureOp
  [op input output]
  (to-predicate (.toVar op) input output))

(defmethod to-predicate IFn
  [op input output]
  (if-let [output (not-empty output)]
    (->Operation (d/mapop* op) input output)
    (->FilterOperation (d/filterop* op) input)))

(defmethod to-predicate ::d/filter
  [op input output]
  (->FilterOperation op input))

(defmethod to-predicate ::d/map
  [op input output]
  (->Operation op input output))

(defmethod to-predicate ::d/mapcat
  [op input output]
  (->Operation op input output))

(defmethod to-predicate CascalogFunction
  [op input output]
  (->Operation op input output))

;; ## Aggregators
;;
;; TODO: Get these impls out and back into the cascading executor.
(defmethod to-predicate ::d/buffer
  [op input output]
  (->Aggregator op input output))

(defmethod to-predicate ::d/bufferiter
  [op input output]
  (->Aggregator op input output))

(defmethod to-predicate ::d/aggregate
  [op input output]
  (->Aggregator op input output))

(defmethod to-predicate ::d/combiner
  [op input output]
  (->Aggregator op input output))

(defmethod to-predicate ParallelAggregator
  [op input output]
  (->Aggregator op input output))

(defmethod to-predicate CascalogBuffer
  [op input output]
  (->Aggregator op input output))

;; TODO: jcascalog ParallelAgg.

(defmethod to-predicate CascalogAggregator
  [op input output]
  (->Aggregator op input output))

(defn build-predicate
  "Accepts an option map and a raw predicate and returns a node in the
  Cascalog graph."
  [options {:keys [op input output] :as pred}]
  (cond (types/generator? op)   (generator-node op input output)
        (instance? Prepared op) (build-predicate options
                                                 (assoc pred :op ((:op op) options)))
        :else                   (to-predicate op input output)))

(comment
  (require '[cascalog.cascading.flow :as f])
  "TODO: Convert to test."
  (let [gen (-> (types/generator [1 2 3 4])
                (ops/rename* "?x"))
        pred (to-predicate * ["?a" "?a"] ["?b"])]
    (fact
      (f/to-memory
       ((:op pred) gen ["?x" "?x"] "?z"))
      => [[1 1] [2 4] [3 9] [4 16]])))
