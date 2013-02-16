(ns cascalog.math.stats
  (:use cascalog.api)
  (:require [cascalog.ops :as c]))

(def variance
  "Predicate macro that calculates the variance of the supplied input
   var."
  (<- [!val :> !var]
      (* !val !val :> !squared)
      (c/sum !squared :> !square-sum)
      (c/count !count)
      (c/avg !val :> !mean)
      (* !mean !mean :> !mean-squared)
      (div !square-sum !count :> !i)
      (- !i !mean-squared :> !var)))

(def sample-variance
  "Predicate macro that calculates the sample variance of the supplied input
   var."
  (<- [!val :> !var]
      (* !val !val :> !squared)
      (c/sum !squared :> !squared-sum)
      (c/count !count)
      (c/sum !val :> !sum)
      (c/avg !val :> !mean)
      (* !sum !mean :> !i)
      (- !squared-sum !i :> !num)
      (- !count 1 :> !denom)
      (div !num !denom :> !var)))

