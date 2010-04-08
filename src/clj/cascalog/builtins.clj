(ns cascalog.builtins
  (:refer-clojure :exclude [count])
  (:require [cascalog [predicate :as p]]))

(p/defcomplexagg count [infields outfields]
  ;; TODO: finish
  )

(p/defcomplexagg sum [infields outfields]
  ;; TODO: finish
  )