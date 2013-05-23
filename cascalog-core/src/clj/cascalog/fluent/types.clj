(ns cascalog.fluent.types
  (:require [cascalog.util :as u]
            [cascalog.fluent.algebra :refer (plus Semigroup)]
            [cascalog.fluent.tap :as tap])
  (:import [cascalog Util]
           [cascalog.fluent.tap CascalogTap]
           [cascading.pipe Pipe Merge]
           [cascading.tap Tap]
           [cascading.tuple Fields Tuple]
           [com.twitter.maple.tap MemorySourceTap]
           [jcascalog Subquery]))

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
  Subquery
  (generator [sq]
    (generator (.getCompiledSubquery sq)))

  CascalogTap
  (generator [tap] (generator (:source tap)))

  clojure.lang.IPersistentVector
  (generator [v] (generator (seq v)))

  nil
  (generator [_]
    (generator
     (MemorySourceTap. [] Fields/ALL)))

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

(defn normalize-sink-connection [sink subquery]
  (cond (fn? sink)  (sink subquery)
        (map? sink) (normalize-sink-connection (:sink sink) subquery)
        :else       [sink subquery]))

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
