;;    Copyright 2010 Nathan Marz
;; 
;;    This program is free software: you can redistribute it and/or modify
;;    it under the terms of the GNU General Public License as published by
;;    the Free Software Foundation, either version 3 of the License, or
;;    (at your option) any later version.
;; 
;;    This program is distributed in the hope that it will be useful,
;;    but WITHOUT ANY WARRANTY; without even the implied warranty of
;;    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;;    GNU General Public License for more details.
;; 
;;    You should have received a copy of the GNU General Public License
;;    along with this program.  If not, see <http://www.gnu.org/licenses/>.

(ns cascalog.ops
  (:refer-clojure :exclude [count min max comp juxt partial])
  (:use cascalog.api
        [jackknife.def :only (defnk)]
        [jackknife.seq :only (collectify)]
        [cascalog.workflow :only (fill-tap!)]
        [cascalog.io :only (with-fs-tmp)])
  (:require [cascalog.util :as u]
            [cascalog.ops-impl :as impl]
            [cascalog.vars :as v]))

;; Operation composition functions

(defn negate [op]
  (<- [:<< !invars :> !true?]
      (op :<< !invars :> !curr?)
      (not !curr? :> !true?)))

(defn all [& ops]
  (impl/logical-comp ops #'impl/bool-and))

(defn any [& ops]
  (impl/logical-comp ops #'impl/bool-or))

(defn comp [& ops]
  (let [[invar & more] (v/gen-nullable-vars (inc (clojure.core/count ops)))
        allvars (list* invar (map vector more))]
    (construct [:<< invar :> (last more)]
               (map (fn [o [invars outvars]]
                      [o :<< invars :>> outvars])
                    (reverse ops)
                    (partition 2 1 allvars)))))

(defn juxt [& ops]
  (let [outvars (v/gen-nullable-vars (clojure.core/count ops))]
    (construct
     [:<< "!invars" :>> outvars]
     (map (fn [o v] [o :<< "!invars" :> v])
          ops
          outvars))))

(defn each [op]
  (predmacro [invars outvars]
             {:pre [(or (zero? (clojure.core/count outvars))
                        (= (clojure.core/count invars)
                           (clojure.core/count outvars)))]}
             (if (empty? outvars)
               (for [i invars]
                 [op i])
               (map (fn [i v] [op i :> v])
                    invars
                    outvars))))

(defn partial [op & args]
  (predmacro
   [invars outvars]
   [[op :<< (concat args invars) :>> outvars]]))

;; Operations to use within queries

(defn re-parse [pattern]
  (mapop [str]
    (re-seq pattern str)))

(defparallelagg count
  :init-var #'impl/one
  :combine-var #'+)

(def sum (each impl/sum-parallel))

(def min (each impl/min-parallel))

(def max (each impl/max-parallel))

(def !count (each impl/!count-parallel))

(defparallelbuf limit
  :hof? true
  :init-hof-var #'impl/limit-init
  :combine-hof-var #'impl/limit-combine
  :extract-hof-var #'impl/limit-extract
  :num-intermediate-vars-fn (fn [infields outfields]
                              (clojure.core/count infields))
  :buffer-hof-var #'impl/limit-buffer)

(def limit-rank
  (merge limit {:buffer-hof-var #'impl/limit-rank-buffer}))

(def avg
  (<- [!v :> !avg]
      (count !c)
      (sum !v :> !s)
      (div !s !c :> !avg)))

(def distinct-count
  (<- [:<< !invars :> !c]
      (:sort :<< !invars)
      (impl/distinct-count-agg :<< !invars :> !c)))

(defn fixed-sample-agg [amt]
  (<- [:<< ?invars :>> ?outvars]
      ((cascalog.ops.RandInt.) :<< ?invars :> ?rand)
      (:sort ?rand)
      (limit [amt] :<< ?invars :>> ?outvars)))

;; Common patterns

(defn lazy-generator
  "Returns a cascalog generator on the supplied sequence of
  tuples. `lazy-generator` serializes each item in the lazy sequence
  into a sequencefile located at the supplied temporary directory, and
  returns a tap into its guts.

  I recommend wrapping queries that use this tap with
  `cascalog.io/with-fs-tmp`; for example,

    (with-fs-tmp [_ tmp-dir]
      (let [lazy-tap (pixel-generator tmp-dir lazy-seq)]
      (?<- (stdout)
           [?field1 ?field2 ... etc]
           (lazy-tap ?field1 ?field2)
           ...)))"
  [tmp-path [tuple :as l-seq]]
  {:pre [(coll? tuple)]}
  (let [tap (:sink (hfs-seqfile tmp-path))
        n-fields (clojure.core/count tuple)]
    (fill-tap! tap l-seq)
    (name-vars tap (v/gen-nullable-vars n-fields))))

(defnk first-n
  "Returns a subquery getting the first n elements from sq it
  finds. Can pass in sorting arguments."
  [gen n :sort nil :reverse false]
  (let [num-fields (num-out-fields gen)
        in-vars  (v/gen-nullable-vars num-fields)
        out-vars (v/gen-nullable-vars num-fields)
        sort-set (if sort (-> sort collectify set) #{})
        sort-vars (if sort
                    (mapcat (fn [f v] (if (sort-set f) [v]))
                            (get-out-fields gen)
                            in-vars))]
    (<- out-vars
        (gen :>> in-vars)
        (:sort :<< sort-vars)
        (:reverse reverse)
        (limit [n] :<< in-vars :>> out-vars))))

(defn fixed-sample
  "Returns a subquery getting a random sample of n elements from the generator"
  [gen n]
  (let [num-fields (num-out-fields gen)
        in-vars  (v/gen-nullable-vars num-fields)
        out-vars (v/gen-nullable-vars num-fields)]
    (<- out-vars
        (gen :>> in-vars)
        ((fixed-sample-agg n) :<< in-vars :>> out-vars))))

;; Helpers to use within ops

(defmacro with-timeout [[ms] & body]
  `(let [^java.util.concurrent.Future f# (future ~@body)]
     (try (.get f# ~ms java.util.concurrent.TimeUnit/MILLISECONDS)
          (catch java.util.concurrent.TimeoutException e#
            (.cancel f# true)
            nil))))
