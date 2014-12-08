(ns cascalog.cascading.types
  (:require [jackknife.core :as u]
            [cascalog.logic.algebra :refer (plus Semigroup)]
            [cascalog.logic.platform :refer (generator compile-query)]
            [cascalog.logic.parse :refer (build-rule)]
            [cascalog.cascading.tap :as tap])
  (:import [cascalog Util]
           [cascalog.cascading.tap CascalogTap]
           [cascading.pipe Pipe Merge]
           [cascading.tap Tap]
           [cascading.tuple Fields Tuple]
           [cascalog.logic.parse TailStruct]
           [cascalog.logic.predicate RawSubquery]
           [com.twitter.maple.tap MemorySourceTap]
           [jcascalog Subquery]))

;; ## Platform

(defrecord CascadingPlatform [])

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

;; ## Generators

;; Note that we need to use getIdentifier on the taps.

;; source-map is a map of identifier to tap, or source. Pipe is the
;; current pipe that the user needs to operate on.

(defrecord ClojureFlow [source-map sink-map trap-map tails pipe name])

(defmethod generator [CascadingPlatform ClojureFlow]
  [x] x)

(defmethod generator [CascadingPlatform Subquery]
  [sq]
  (generator (.getCompiledSubquery sq)))

(defmethod generator [CascadingPlatform CascalogTap]
  [tap]
  (generator (:source tap)))

(defmethod generator [CascadingPlatform clojure.lang.IPersistentVector]
  [v]
  (generator (or (seq v) ())))

(defmethod generator [CascadingPlatform clojure.lang.ISeq]
  [v]
  (generator
   (MemorySourceTap. (map to-tuple v) Fields/ALL)))

(defmethod generator [CascadingPlatform java.util.ArrayList]
  [coll]
  (generator (into [] coll)))

(defmethod generator [CascadingPlatform Tap]
  [tap]
  (let [id (u/uuid)]
    (ClojureFlow. {id tap} nil nil nil (Pipe. id) nil)))

(defmethod generator [CascadingPlatform TailStruct]
  [sq]
  (compile-query sq))

(defmethod generator [CascadingPlatform RawSubquery]
  [sq]
  (generator (build-rule sq)))

;; ## Sink Typeclasses

(defprotocol ISink
  (to-sink [this]
    "Returns a Cascading tap into which Cascalog can sink the supplied
    data."))

;; => Tap, Tap => T

(extend-protocol ISink
  Tap
  (to-sink [tap] tap)

  CascalogTap
  (to-sink [tap] (to-sink (:sink tap))))

(defn array-of [t]
  (.getClass
   (java.lang.reflect.Array/newInstance t 0)))

(extend-protocol Semigroup
  (array-of Pipe)
  (plus [l r]
    (into-array Pipe (concat l r)))

  Pipe
  (plus [l r]
    (Merge. (into-array Pipe [(Pipe. (u/uuid) l)
                              (Pipe. (u/uuid) r)])))

  ClojureFlow
  (plus [l r]
    (letfn [(merge-k [k] (merge (k l) (k r)))
            (plus-k [k] (plus (k l) (k r)))]
      (ClojureFlow. (merge-k :source-map)
                     (plus-k :sink-map)
                     (merge-k :trap-map)
                     (plus-k (comp vec :tails))
                     (plus-k :pipe)
                     (:name l)))))
