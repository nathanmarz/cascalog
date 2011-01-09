 ;    Copyright 2010 Nathan Marz
 ; 
 ;    This program is free software: you can redistribute it and/or modify
 ;    it under the terms of the GNU General Public License as published by
 ;    the Free Software Foundation, either version 3 of the License, or
 ;    (at your option) any later version.
 ; 
 ;    This program is distributed in the hope that it will be useful,
 ;    but WITHOUT ANY WARRANTY; without even the implied warranty of
 ;    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 ;    GNU General Public License for more details.
 ; 
 ;    You should have received a copy of the GNU General Public License
 ;    along with this program.  If not, see <http://www.gnu.org/licenses/>.

(ns cascalog.ops
  (:refer-clojure :exclude [count min max])
  (:use [cascalog ops-impl api util])
  (:require [cascalog [vars :as v]])
  (:import [java.util.concurrent Future TimeoutException TimeUnit]))

;; Operations to use within queries

(defmapop [re-parse [pattern]] [str]
  (re-seq pattern str))

(defparallelagg count :init-var #'one
                      :combine-var #'+)

(defparallelagg sum :init-var #'identity-tuple
                    :combine-var #'+-all)

(defparallelagg min :init-var #'identity-tuple
                    :combine-var #'min-all)

(defparallelagg max :init-var #'identity-tuple
                    :combine-var #'max-all)

(defparallelagg !count :init-var #'existence-int-all
                       :combine-var #'+-all)

(defparallelbuf limit :hof? true
                      :init-hof-var #'limit-init
                      :combine-hof-var #'limit-combine
                      :extract-hof-var #'limit-extract
                      :num-intermediate-vars-fn (fn [infields outfields] (clojure.core/count infields))
                      :buffer-hof-var #'limit-buffer )

(def limit-rank (merge limit {:buffer-hof-var #'limit-rank-buffer} ))

(def avg
  (<- [!v :> !avg]
    (count !c) (sum !v :> !s) (div !s !c :> !avg)))

(def distinct-count
  (<- [!v :> !c]
    (:sort !v) (distinct-count-agg !v :> !c)))

; should be able to do this kind of destructuring:
; (def distinct-count (<- [:<< [& vars] :> !c] (:sort :<< vars) (distinct-count-agg :<< vars :> !c)))


;; Common patterns

(defn first-n
  "Returns a subquery getting the first n elements from sq it finds. Can pass in sorting arguments."
  [gen n & kwargs]
  (let [kwargs (merge {:reverse false :sort nil} (apply hash-map kwargs))
        in-fields (get-out-fields gen)
        out-fields (v/gen-nullable-vars (clojure.core/count in-fields))]
    (<- out-fields
        (gen :>> in-fields)
        (:sort :<< (collectify (:sort kwargs))) (:reverse (:reverse kwargs))
        (limit [n] :<< in-fields :>> out-fields)
        )))

;; Helpers to use within ops

(defmacro with-timeout [[ms] & body]
  `(let [#^Future f# (future ~@body)]
     (try
       (.get f# ~ms TimeUnit/MILLISECONDS)
     (catch TimeoutException e#
       (.cancel f# true)
       nil
       ))))
