(ns cascalog.cascading-api-secondary-test
    (:use clojure.test
          cascalog.api
          cascalog.logic.testing
          cascalog.cascading.testing)
    (:import [cascading.tuple Fields])
    (:require [cascalog.logic.ops :as c]
              [cascalog.cascading.io :as io]))

(use-fixtures :once
  (fn  [f]
    (set-cascading-platform!)
    (f)))



