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
  (:use [cascalog vars util graph])
  (:import [cascading.tuple Fields])
  (:require [cascalog [workflow :as w] [predicate :as p]]))

(defn one [] 1)

(w/defmapop [re-parse [pattern]] [str]
  (re-seq pattern str))

(p/defparallelagg count :init-var #'one
                        :combine-var #'+
                        :args 0)

(p/defparallelagg sum :init-var #'identity
                      :combine-var #'+
                      :args 1)

(p/defparallelagg min :init-var #'identity
                      :combine-var #'clojure.core/min
                      :args 1)

(p/defparallelagg max :init-var #'identity
                      :combine-var #'clojure.core/max
                      :args 1)

(defn existence-int [v] (if v 1 0))

(p/defparallelagg !count :init-var #'existence-int
                         :combine-var #'+
                         :args 1)

(defn limit-init [options limit]
  (fn [sort-tuple & tuple]
    [[(vec sort-tuple) (vec tuple)]]))

(defn- mk-limit-comparator [options]
  (fn [[#^Comparable o1 _] [#^Comparable o2 _]]
    (if (:sort options)
      (* (.compareTo o1 o2) (if (boolean (:reverse options)) -1 1))
      0 )))

(defn limit-combine [options limit]
  (let [compare-fn (mk-limit-comparator options)]
   (fn [list1 list2]
      (let [res (concat list1 list2)]
        (if (> (clojure.core/count res) (* 2 limit))
          (take limit (sort compare-fn res))
          res
          ))
      )))

(defn limit-extract [options limit]
  (let [compare-fn (mk-limit-comparator options)]
  (fn [alist]
    (let [alist (if (<= (clojure.core/count alist) limit) alist (take limit (sort compare-fn alist)))]
      (map (partial apply concat) alist)))))

(defn limit-buffer [options limit]
  (fn [tuples]
    (take limit tuples)))

(p/defparallelbuf limit :hof? true
                        :init-hof-var #'limit-init
                        :combine-hof-var #'limit-combine
                        :extract-hof-var #'limit-extract
                        :num-intermediate-vars-fn (fn [infields outfields] (clojure.core/count infields))
                        :buffer-hof-var #'limit-buffer )

(defn limit-rank-buffer [options limit]
  (fn [tuples]
    (take limit (map #(conj (vec %1) %2) tuples (iterate inc 1)))))

(def limit-rank (merge limit {:buffer-hof-var #'limit-rank-buffer} ))