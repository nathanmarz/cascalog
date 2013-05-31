(ns cascalog.fluent.types
  (:require [cascalog.util :as u]
            [cascalog.fluent.algebra :refer (plus Semigroup)]
            [cascalog.fluent.tap :as tap])
  (:import [cascalog Util]
           [cascalog.fluent.tap CascalogTap]
           [cascading.pipe Pipe Merge]
           [cascading.tap Tap]
           [cascading.tuple Fields Tuple]
           [com.twitter.maple.tap MemorySourceTap]))

;; ## Tuple Conversion
;;
;; TODO: We should probably have our tuple converter typeclass follow
;; the same pattern as scalding. Go to the tuple, come back from the
;; tuple. Accomplish this with a from-tuple method.

(defprotocol ITuple
  (to-tuple [this]
    "Returns a tupled representation of the supplied thing."))

(extend-protocol ITuple
  Tuple
  (to-tuple [t] t)

  clojure.lang.IPersistentVector
  (to-tuple [v] (Util/coerceToTuple v)) ;; TODO: do this in clojure.

  Object
  (to-tuple [v] (to-tuple [v])))

;; ## Generator Protocol

(defprotocol IGenerator
  "Accepts some type and returns a ClojureFlow that can be used as a
  generator. The idea is that a clojure flow can always be used
  directly as a generator."
  (generator [x]))

(defn generator?
  "Returns true if the supplied item can be used as a Cascalog
  generator, false otherwise."
  [x]
  (satisfies? IGenerator x))

;; Note that we need to use getIdentifier on the taps.

;; source-map is a map of identifier to tap, or source. Pipe is the
;; current pipe that the user needs to operate on.

(defrecord ClojureFlow [source-map sink-map trap-map tails pipe]
  IGenerator
  (generator [x] x))

(extend-protocol IGenerator
  CascalogTap
  (generator [tap] (generator (:source tap)))

  clojure.lang.IPersistentVector
  (generator [v] (generator (seq v)))
  clojure.lang.ISeq
  (generator [v]
    (generator
     (MemorySourceTap. (map to-tuple v) Fields/ALL)))

  java.util.ArrayList
  (generator [coll]
    (generator (into [] coll)))

  Tap
  (generator [tap]
    (let [id (.getIdentifier tap)]
      (map->ClojureFlow {:source-map {id tap}
                         :pipe (Pipe. id)}))))

;; ## Sink Typeclasses

(defprotocol ISink
  (to-sink [this]
    "Returns a Cascading tap into which Cascalog can sink the supplied
    data."))

;; => Tap, Tap => T

(extend-protocol ISink
  Tap
  (to-sink [tap] tap)

  ;; old cascalog-tap. Deprecate this soon.
  clojure.lang.PersistentStructMap
  (to-sink [tap] (to-sink (:sink tap)))

  CascalogTap
  (to-sink [tap] (to-sink (:sink tap))))

(defn- uniqify
  [pipe]
  (Pipe. (u/uuid) pipe))

(extend-protocol Semigroup
  Pipe
  (plus [l r]
    (Merge. (into-array Pipe [(Pipe. (u/uuid) l)
                              (Pipe. (u/uuid) r)])))

  ClojureFlow
  (plus [l r]
    (letfn [(merge-k [k] (merge (k l) (k r)))
            (plus-k [k] (plus (k l) (k r)))]
      (->ClojureFlow (merge-k :source-map)
                     (plus-k :sink-map)
                     (plus-k :trap-map)
                     (plus-k :tails)
                     (plus-k :pipe)))))

(defn with-merged-pipes
  "Creates a new flow by merging incoming pipes using f,
  which should be a function Pipe[] => Pipe"
  [flows f]
  (letfn [(merge-k [k] (apply merge (map k flows)))
          (merge-pipes [k] (into-array Pipe (map (comp uniqify k) flows)))]
    (->ClojureFlow (merge-k :source-map)
                   (merge-k :sink-map)
                   (merge-k :trap-map)
                   (vec (mapcat :tails flows))
                   (f (merge-pipes :pipe)))))