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
  (:refer-clojure :exclude [count min max comp juxt])
  (:use [cascalog ops-impl api util])
  (:use [clojure.contrib.def :only [defnk]])
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
  (<- [:<< !invars :> !c]
    (:sort :<< !invars) (distinct-count-agg :<< !invars :> !c)))


;; Operation composition functions

(defn negate [op]
  (<- [:<< !invars :> !true?]
      (op :<< !invars :> !curr?)
      (not !curr? :> !true?)
      ))

(defn all [& ops]
  (logical-comp ops #'bool-and))

(defn any [& ops]
  (logical-comp ops #'bool-or))

(defn comp [& ops]
  (let [ops (reverse ops)
        intvars (map vector (v/gen-nullable-vars (dec (clojure.core/count ops))))
        intvars (vec (cons "!invars" intvars))
        allvars (conj intvars ["!result"])
        varpairs (partition 2 1 allvars)
        ]
    (construct
     [:<< "!invars" :> "!result"]
     (map (fn [o [invars outvars]] [o :<< invars :>> outvars]) ops varpairs)
     )))

(defn juxt [& ops]
  (let [outvars (v/gen-nullable-vars (clojure.core/count ops))]
    (construct
     [:<< "!invars" :>> outvars]
     (map (fn [o v] [o :<< "!invars" :> v]) ops outvars))
    ))


;; Common patterns

(defnk first-n
  "Returns a subquery getting the first n elements from sq it finds. Can pass in sorting arguments."
  [gen n :reverse false :sort nil]
  (let [in-fields (get-out-fields gen)
        out-fields (v/gen-nullable-vars (clojure.core/count in-fields))]
    (<- out-fields
        (gen :>> in-fields)
        (:sort :<< (when sort (collectify sort)))
        (:reverse reverse)
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
