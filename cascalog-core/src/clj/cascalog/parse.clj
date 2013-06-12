(ns cascalog.parse
  (:require [clojure.string :refer (join)]
            [clojure.set :refer (difference intersection)]
            [clojure.walk :refer (postwalk)]
            [jackknife.core :refer (throw-illegal throw-runtime)]
            [jackknife.seq :as s]
            [cascalog.vars :as v]
            [cascalog.util :as u]
            [cascalog.predicate :as p]
            [cascalog.options :as opts]
            [cascalog.rules :as rules]
            [cascalog.fluent.types :refer (generator?)]
            [cascalog.fluent.def :as d :refer (buffer? aggregator?)]
            [cascalog.fluent.operations :as ops]
            [cascalog.fluent.cascading :refer (uniquify-var)])
  (:import [jcascalog Predicate PredicateMacro PredicateMacroTemplate]
           [cascalog.predicate Operation FilterOperation Aggregator Generator]
           [clojure.lang IPersistentVector]))

(defprotocol IRawPredicate
  (normalize [_]
    "Returns a sequence of RawPredicate instances."))

;; Raw Predicate type.

(defrecord RawPredicate [op input output]
  IRawPredicate
  (normalize [p] [p]))

;; ## Predicate Macro Building Functions

;; TODO: expand should return a vector of RawPredicate instances.
;;
;; "expand" is called from "normalize" in cascalog.parse. The parsing
;;  code takes care of the recursive expansion needed on the results
;;  of a call to "expand".

(defprotocol IPredMacro
  (expand [_ input output]
    "Returns a sequence of vectors suitable to feed into
    cascalog.parse/normalize."))

(extend-protocol p/ICouldFilter
  cascalog.parse.IPredMacro
  (filter? [_] true))

(extend-protocol IPredMacro
  ;; Predicate macro templates really should just extend this protocol
  ;; directly. getCompiledPredMacro calls into build-predmacro below
  ;; and returns a reified instance of IPredMacro.
  PredicateMacroTemplate
  (expand [p input output]
    ((.getCompiledPredMacro p) input output))

  ;; TODO: jCascalog shold just use these interfaces directly. If this
  ;; were the case, we wouldn't have to extend the protocol here.
  PredicateMacro
  (expand [p input output]
    (letfn [(to-fields [fields]
              (jcascalog.Fields. (or fields [])))]
      (-> p (.getPredicates (to-fields input)
                            (to-fields output))))))

(defn predmacro? [o]
  (satisfies? IPredMacro o))

;; kind of a hack, simulate using pred macros like filters

(defn use-as-filter?
  "If a predicate macro had a single output variable defined and you
  try to use it with no output variables, the predicate macro acts as
  a filter."
  [input-decl output-decl outvars]
  (and (empty? outvars)
       (sequential? input-decl)
       (= 1 (count output-decl))))

(defn predmacro*
  "Functional version of predmacro. See predmacro for details."
  [fun]
  (reify IPredMacro
    (expand [_ invars outvars]
      (fun invars outvars))))

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

(defn validate-declarations!
  "Assert that the same variables aren't used on input and output when
  defining a predicate macro."
  [input-decl output-decl]
  (when (seq (intersection (set input-decl)
                           (set output-decl)))
    ;; TODO: ignore destructuring characters and check that no
    ;; constants are present.
    (throw-runtime (format
                    (str "Cannot declare the same var as "
                         "an input and output to predicate macro: %s %s")
                    input-decl output-decl))))

(defn build-predmacro
  "Build a predicate macro via input and output declarations. This
  function takes a sequence of declared inputs, a seq of declared
  outputs and a sequence of raw predicates. Upon use, any variable
  name not in the input or output declarations will be replaced with a
  random Cascalog variable (uniqued by appending a suffix, so nullable
  vs non-nullable will be maintained)."
  [input-decl output-decl raw-predicates]
  (validate-declarations! input-decl output-decl)
  (reify IPredMacro
    (expand [_ invars outvars]
      (let [outvars (if (use-as-filter? input-decl output-decl outvars)
                      [true]
                      outvars)
            replacement-m (u/mk-destructured-seq-map input-decl invars
                                                     output-decl outvars)
            update (memoize (fn [v]
                              (if (v/cascalog-var? v)
                                (replacement-m (str v) (uniquify-var v))
                                v)))]
        (mapcat (fn [pred]
                  (map (fn [{:keys [input output] :as p}]
                         (-> p
                             (assoc :input  (postwalk update input))
                             (assoc :output (postwalk update output))))
                       (normalize pred)))
                raw-predicates)))))

;; ## Variable Parsing

;; TODO: Note that this is the spot where we'd go ahead and add new
;; selectors to Cascalog. For example, say we wanted the ability to
;; pour the results of a query into a vector directly; :>> ?a. This is
;; the place.

;; TODO: validation on the arg-m. We shouldn't ever have the sugar arg
;; and the non-sugar arg. Move the examples out to tests.

(defn desugar-selectors
  "Accepts a map of cascalog input or output symbol (:< or :>, for
  example) to var sequence, a <sugary input or output selector> and a
  <full vector input or output selector> and either destructures the
  non-sugary input or moves the sugary input into its proper
  place. For example:

 (desugar-selectors {:>> ([\"?a\"])} :> :>>)
 ;=> {:>> [\"?a\"]}

 (desugar-selectors {:> [\"?a\"] :<< [[\"?b\"]]} :> :>> :< :<<)
 ;=>  {:>> [\"?a\"], :<< [\"?b\"]}"
  [arg-m & sugar-full-pairs]
  (letfn [(desugar [m [sugar-k full-k]]
            (if-not (some m #{sugar-k full-k})
              m
              (-> m
                  (dissoc sugar-k)
                  (assoc full-k
                    (or (first (m full-k))
                        (m sugar-k))))))]
    (reduce desugar arg-m
            (partition 2 sugar-full-pairs))))

(defn expand-positional-selector
  "Accepts a map of cascalog selector to var sequence and, if the map
  contains an entry for Cascalog's positional selector, expands out
  the proper number of logic vars and replaces each entry specified
  within the positional map. This function returns the updated map."
  [arg-m]
  (if-let [[var-count selector-map] (:#> arg-m)]
    (let [expanded-vars (reduce (fn [v [pos var]]
                                  (assoc v pos var))
                                (vec (v/gen-nullable-vars var-count))
                                selector-map)]
      (-> arg-m
          (dissoc :#>)
          (assoc :>> expanded-vars)))
    arg-m))

(defn to-map
  "Accepts a sequence of alternating [single-k, many-values] pairs and
  returns a map of k -> vals."
  [k? elems]
  (let [[keys vals] (s/unweave (partition-by k? elems))]
    (zipmap (map first keys) vals)))

(defn parse-variables
  "parses variables of the form ['?a' '?b' :> '!!c'] and returns a map
   of input variables, output variables, If there is no :>, defaults
   to selector-default."
  [vars default-selector]
  {:pre [(contains? #{:> :<} default-selector)]}
  (let [vars (cond (v/selector? (first vars)) vars
                   (some v/selector? vars) (cons :< vars)
                   :else (cons default-selector vars))
        {input :<< output :>>} (-> (to-map v/selector? vars)
                                   (desugar-selectors :> :>>
                                                      :< :<<)
                                   (expand-positional-selector))]
    {:input  (v/sanitize input)
     :output (v/sanitize output)}))

(defn default-selector
  "Default selector (either input or output) for this
  operation. Dispatches based on type."
  [op]
  (if (p/filter? op) :< :>))

(defn split-outvar-constants
  "Accepts a sequence of output variables and returns a 2-vector:

  [new-outputs, [seq-of-new-raw-predicates]]

  By creating a new output predicate for every constant in the output
  field."
  [output]
  (reduce (fn [[new-output pred-acc] v]
            (if (v/cascalog-var? v)
              [(conj new-output v) pred-acc]
              (let [newvar (v/gen-nullable-var)]
                [(conj new-output newvar)
                 (conj pred-acc
                       (map->RawPredicate (if (or (fn? v) (u/multifn? v))
                                            {:op v :input [newvar]}
                                            {:op = :input [v newvar]})))])))
          [[] []]
          output))

(extend-protocol IRawPredicate
  Predicate
  (normalize [p]
    (normalize (into [] (.toRawCascalogPredicate p))))

  IPersistentVector
  (normalize [[op & rest]]
    (let [default (default-selector op)
          {:keys [input output]} (parse-variables rest (default-selector op))
          [output preds] (split-outvar-constants output)]
      (concat preds (if (predmacro? op)
                      (mapcat normalize (expand op input output))
                      [(->RawPredicate op
                                       (not-empty input)
                                       (not-empty output))])))))

;; ## Unground Var Validation

(defn unground-outvars
  "For the supplied sequence of RawPredicate instances, returns a seq
  of all ungrounding vars in the output position."
  [predicates]
  (mapcat (comp (partial filter v/unground-var?) :output)
          predicates))

(defn unground-assertions!
  "Performs various validations on the supplied set of parsed
  predicates. If all validations pass, returns the sequence
  unchanged."
  [gens ops]
  (let [gen-outvars (unground-outvars gens)
        extra-vars  (difference (set (unground-outvars ops))
                                (set gen-outvars))
        dups (s/duplicates gen-outvars)]
    (when (not-empty extra-vars)
      (throw-illegal (str "Ungrounding vars must originate within a generator. "
                          extra-vars
                          " violate(s) the rules.")))
    (when (not-empty dups)
      (throw-illegal (str "Each ungrounding var can only appear once per query."
                          "The following are duplicated: "
                          dups)))))

(defn aggregation-assertions! [buffers aggs]
  (when (and (not-empty aggs)
             (not-empty buffers))
    (throw-illegal "Cannot use both aggregators and buffers in same grouping"))
  (when (> (count buffers) 1)
    (throw-illegal "Multiple buffers aren't allowed in the same subquery.")))

;; Output of the subquery, the predicates it contains and the options
;; in the subquery.
;;
;; TODO: Have this thing extend IGenerator.
(defrecord RawSubquery [fields predicates options])

;; Printing Methods
;;
;; The following methods allow a predicate to print properly.

(defmethod print-method RawPredicate
  [{:keys [op input output]} ^java.io.Writer writer]
  (binding [*out* writer]
    (let [op (if (ifn? op)
               (let [op (or (:cascalog.fluent.def/op (meta op)) op)]
                 (or (u/search-for-var op)
                     op))
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
  [{:keys [fields predicates options]} ^java.io.Writer writer]
  (binding [*out* writer]
    (println "(<-" (vec fields))
    (doseq [pred predicates]
      (print "    ")
      (print-method pred writer))
    (doseq [[k v] options :when (not (nil? v))]
      (println (format "    (%s %s)" k v)))
    (println "    )")))

(defn validate-predicates! [preds]
  (let [grouped (group-by (fn [x]
                            (condp #(%1 %2) (:op x)
                              generator? :gens
                              buffer? :buffers
                              aggregator? :aggs
                              :ops))
                          preds)]
    (unground-assertions! (:gens grouped)
                          (:ops grouped))
    (aggregation-assertions! (:buffers grouped)
                             (:aggs grouped))))

(defn query-signature?
  "Accepts the normalized return vector of a Cascalog form and returns
  true if the return vector is from a subquery, false otherwise. (A
  predicate macro would trigger false, for example.)"
  [vars]
  (not (some v/selector? vars)))

(comment
  (v/with-logic-vars
    (parse-subquery [?x ?y ?z]
                    [[[[1 2 3]] ?x]
                     [* ?x ?x :> ?y]
                     [* ?x ?y :> ?z]])))

;; ## Predicate Parsing
;;
;;
;; Before compilation, all predicates are normalized down to clojure
;; predicates.
;;
;; Query compilation steps are as follows:
;;
;; 1. Desugar all of the argument selectors (remember positional!)
;; 2. Normalize all predicates
;; 3. Expand predicate macros

(defn parse-subquery
  "Parses predicates and output fields and returns a proper subquery."
  [output-fields raw-predicates]
  (let [output-fields (v/sanitize output-fields)
        [raw-options raw-predicates] (s/separate opts/option? raw-predicates)
        option-map (opts/generate-option-map raw-options)
        raw-predicates (mapcat normalize raw-predicates)]
    (if (query-signature? output-fields)
      (do (validate-predicates! raw-predicates)
          (->RawSubquery output-fields
                         raw-predicates
                         option-map))
      (let [parsed (parse-variables output-fields :<)]
        (build-predmacro (:input parsed)
                         (:output parsed)
                         raw-predicates)))))

(defmacro <-
  "Constructs a query or predicate macro from a list of
  predicates. Predicate macros support destructuring of the input and
  output variables."
  [outvars & predicates]
  `(v/with-logic-vars
     (parse-subquery ~outvars [~@(map vec predicates)])))

;; TODO: Implement IGenerator here.

(defrecord TailStruct [value ground? operations available-fields parents])

;; ## Operation Application

(defprotocol IApplyToTail
  (accept? [this tail]
    "Returns true if this op can be applied to the current tail")

  (apply-to-tail [this tail]
    "Accepts a tail and performs some modification on that tail,
    returning a new tail."))

;; Sort by filter vs operation, then try and apply all as a fixed
;; point. Write the fixed point application in terms of the tail.

;; TODO: Implement this as a search through the zipper.

(defn find-generator-join-set-vars
  "Returns a singleton vector with the generator-as-set join var if it
  exists, nil otherwise."
  [node]
  (let [predicate     (g/get-value node)
        inbound-nodes (g/get-inbound-nodes node)
        pred-type     (:type predicate)]
    (condp = pred-type
      :join       nil
      :group      nil
      :generator (if-let [v (:join-set-var predicate)] [v])
      (if (= 1 (count inbound-nodes))
        (recur (first inbound-nodes))
        (throw-runtime "Planner exception: Unexpected number of "
                       "inbound nodes to non-generator predicate.")))))

;; ## Operation Application

(letfn [
        ;; The assumption here is that the new operation is only
        ;; ADDING fields. All output fields are unique, in this
        ;; particular case.
        ;;
        ;; We have a bug here, because input fields show up before
        ;; output fields -- if we write a certain input field BEFORE
        ;; an output field, we lose.
        (connect-op [tail op]
          (->  tail
               (update-in [:available-fields]
                          #(concat % (:output op)))
               (assoc :parents [tail])))

        ;; Adds an operation onto the tail -- this builds up an entry
        ;; in the graph by connecting the new operation to the old
        ;; operation's node. This is how we keep track of everything.
        (add-op [tail op]
          (let [tail    (connect-op tail op)
                new-ops (s/remove-first #{op} (:operations tail))]
            (merge tail {:operations new-ops})))

        (op-allowed? [tail op]
          (let [ground?          (:ground? tail)
                available-fields (:available-fields tail)
                join-set-vars    (find-generator-join-set-vars (:node tail))
                infields-set     (set (:infields op))]
            (and (or (:allow-on-genfilter? op)
                     (empty? join-set-vars))
                 (subset? infields-set (set available-fields))
                 (or ground? (every? v/ground-var? infields-set)))))

        ;; Adds operations to tail until can't anymore. Returns new tail
        (fixed-point-operations [tail]
          (if-let [op (s/find-first (partial op-allowed? tail)
                                    (:operations tail))]
            (recur (add-op tail op))
            tail))

        (add-drift-op [tail equality-sets rename-map new-drift-map]
          (let [eq-assemblies (map w/equal equality-sets)
                outfields (vec (keys rename-map))
                rename-in (vec (vals rename-map))
                rename-assembly (if (seq rename-in)
                                  (w/identity rename-in :fn> outfields :> Fields/SWAP)
                                  identity)
                assembly   (apply w/compose-straight-assemblies
                                  (concat eq-assemblies [rename-assembly]))
                infields (vec (apply concat rename-in equality-sets))
                tail (connect-op tail (p/predicate p/operation
                                                   assembly
                                                   infields
                                                   outfields
                                                   false))
                newout (difference (set (:available-fields tail))
                                   (set rename-in))]
            (merge tail {:drift-map new-drift-map
                         :available-fields newout} )))

        (drift-equalities [drift-m available-fields]
          (let [eqmap (select-keys (u/reverse-map
                                    (select-keys drift-m available-fields))
                                   available-fields)
                equality-sets (map (fn [[k v]] (conj v k)) eqmap)]))

        (determine-drift
          [drift-map available-fields]
          ;; This function kicks out a new drift map, a set of fields
          ;; that are meant to be equal, and a map of items to rename.
          (let [available-set (set available-fields)
                rename-map (reduce (fn [m f]
                                     (let [drift (drift-map f)]
                                       (if (and drift (not (available-set drift)))
                                         (assoc m drift f)
                                         m)))
                                   {} available-fields)
                eqmap (select-keys (u/reverse-map
                                    (select-keys drift-map available-fields))
                                   available-fields)
                equality-sets (map (fn [[k v]] (conj v k)) eqmap)
                new-drift-map (->> equality-sets
                                   (apply concat (vals rename-map))
                                   (apply dissoc drift-map))]
            (prn "NEW DRIFT: " new-drift-map)
            [new-drift-map equality-sets rename-map]))]

  (defn- add-ops-fixed-point
    "Adds operations to tail until can't anymore. Returns new tail"
    [tail]
    (let [{:keys [drift-map available-fields] :as tail} (fixed-point-operations tail)
          [new-drift-map equality-sets rename-map]
          (determine-drift drift-map available-fields)]
      (if (and (empty? equality-sets)
               (empty? rename-map))
        tail
        (recur (add-drift-op tail equality-sets rename-map new-drift-map))))))

(defn merge-tails
  "The first call begins with an empty graph and a bunch of generator
  tails, each with a list of operations that could be applied. Based
  on the op-allowed logic, these tails try to consume as many
  operations as possible before giving up at a fixed point."
  [tails]
  ;; Start by adding as many operations as possible to each tail.
  (prn tails)
  (let [tails (map add-ops-fixed-point tails)]
    (if (= 1 (count tails))
      ;; once we get down to a single generator -- either because we
      ;; only had one to begin with, or because a join gets us back to
      ;; basics -- go ahead and mark the tail as ground? and apply the
      ;; rest of the operations.
      (add-ops-fixed-point (assoc (first tails) :ground? true))
      (recur (attempt-join tails)))))

(defn build-rule [{:keys [fields predicates options]}]
  (let [predicates (map p/build-predicate predicates)
        grouped    (group-by type predicates)
        generators (grouped Generator)
        filters    (grouped FilterOperation)
        ops        (grouped Operation)
        aggs       (grouped Aggregator)
        joined     (merge-tails
                    (map (fn [gen]
                           (->TailStruct (v/fully-ground? (:fields gen))
                                         (concat filters ops)
                                         (:fields gen)
                                         gen))
                         generators))]
    [generators filters ops aggs]))

(comment
  (let [x (<- [?x ?y :> ?z]
              (* ?x ?x :> 10)
              (* ?x ?y :> ?z))
        sq (<- [?a ?b ?z]
               ([[1 2 3]] ?a)
               (x ?a ?a :> 4)
               ((d/parallelagg* +) ?a :> ?z)
               ((d/bufferop* +) ?a :> ?z)
               ((cascalog.fluent.def/mapcatop* +) ?a 10 :> ?b))]
    (clojure.pprint/pprint (build-rule sq))))
