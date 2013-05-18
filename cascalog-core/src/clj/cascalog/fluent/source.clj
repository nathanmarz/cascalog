(ns cascalog.fluent.source
  (:require cascalog.fluent.tap)
  (:import [cascalog Util]
           [cascalog.fluent.tap CascalogTap]
           [cascading.tap Tap]
           [cascading.tuple Fields Tuple]
           [com.twitter.maple.tap MemorySourceTap]))

;; ## Tuple Methods
