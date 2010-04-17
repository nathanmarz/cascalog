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