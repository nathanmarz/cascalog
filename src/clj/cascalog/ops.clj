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

;; TODO: do the pre-group "fake combiners" optimization for everything here

(defn- verify-args [opname infields outfields expected-in expected-out]
  (when (or (not= expected-in (clojure.core/count infields))
            (not= expected-out (clojure.core/count outfields)))
    (throw (IllegalArgumentException. (str "Invalid args to " opname infields outfields))))
  )

(p/defcomplexagg count [infields outfields]
  (verify-args "count" infields outfields 0 1)
  [identity (w/count (first outfields))])

(p/defcomplexagg sum [infields outfields]
  (verify-args "sum" infields outfields 1 1)
  [identity (w/sum (first infields) (first outfields))])

(p/defcomplexagg min [infields outfields]
  (verify-args "min" infields outfields 1 1)
  [identity (w/min (first infields) (first outfields))])

(p/defcomplexagg max [infields outfields]
  (verify-args "max" infields outfields 1 1)
  [identity (w/max (first infields) (first outfields))])

(w/defmapop existence?-int [v]
  (if v 1 0))

;; TODO: this would be more efficient with a custom aggregator
(p/defcomplexagg !count [infields outfields]
  (verify-args "!count" infields outfields 1 1)
  (let [val-var (gen-nullable-var)]
    [(existence?-int (first infields) :fn> val-var :> Fields/ALL)
     (w/sum val-var (first outfields))]
    ))