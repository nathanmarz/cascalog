(ns cascalog.fluent.source
  (:require cascalog.fluent.tap)
  (:import [cascalog Util]
           [cascalog.fluent.tap CascalogTap]
           [cascading.tap Tap]
           [cascading.tuple Fields]
           [com.twitter.maple.tap MemorySourceTap]))

;; ## Tuple Methods

(defprotocol ITuple
  (to-tuple [this]
    "Returns a tupled representation of the supplied thing."))

(extend-protocol ITuple
  clojure.lang.IPersistentVector
  (to-tuple [v] (Util/coerceToTuple v)) ;; TODO: do this in clojure.

  Object
  (to-tuple [v] (to-tuple [v])))

;; ## Source Methods

;; TODO: Does this type hint do anything?

(defprotocol ISource
  (to-source ^Tap [this]
    "Returns a Cascading tap that allows access to the supplied
    data."))

(extend-protocol ISource
  Tap
  (to-source [tap] tap)

  CascalogTap
  (to-source [tap] (to-source (:source tap)))

  clojure.lang.IPersistentVector
  (to-source [v]
    (MemorySourceTap. (map to-tuple v)
                      Fields/ALL))

  java.util.ArrayList
  (to-source [coll]
    (to-source (into [] coll))))

(defprotocol ISink
  (to-sink [this]
    "Returns a Cascading tap into which Cascalog can sink the supplied
    data."))

;; => Tap, Tap => T

(extend-protocol ISink
  Tap
  (to-sink [tap] tap)

  ;; old cascalog-tap.
  clojure.lang.PersistentStructMap
  (to-sink [tap] (to-sink (:sink tap)))

  CascalogTap
  (to-sink [tap] (to-sink (:sink tap))))
