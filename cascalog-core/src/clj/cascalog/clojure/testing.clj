(ns cascalog.clojure.testing
  (:require [cascalog.api :refer :all]
            [cascalog.logic.testing :refer (ITestable process?-)]
            [jackknife.seq :refer (unweave)])
  (:import [cascalog.clojure.platform ClojurePlatform]))

(extend-protocol ITestable
  ClojurePlatform
  (process?- [_ [ll :as bindings]]
    (let [bindings (if (keyword? ll)
                     (rest bindings)
                     bindings)
          [specs rules] (unweave bindings)
          out-tuples (apply ??- rules)]
      [specs out-tuples])))
