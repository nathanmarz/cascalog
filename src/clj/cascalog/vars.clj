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

(ns cascalog.vars)

;; TODO: better to use UUIDs to avoid name collisions with client code?
;; Are the size of fields an issue in the actual flow execution perf-wise?
(let [i (atom 0)]
  (defn gen-unique-suffix [] (str "__gen" (swap! i inc))))

(defn gen-nullable-var [] (str "!" (gen-unique-suffix)))

(defn- extract-varname [v]
  (let [actname (if (symbol? v) (name v) v)]
    (if (= "_" actname) (gen-nullable-var) actname)))

(defn cascalog-var? [obj]
    (if (or (symbol? obj) (string? obj))
      (let [obj (extract-varname obj)]
          ((complement nil?) (some #(.startsWith obj %) ["?" "!" "!!"])))
      false ))

(defn non-nullable-var? [sym-or-str]
  (.startsWith (extract-varname sym-or-str) "?"))

(def nullable-var? (complement non-nullable-var?))

(defn unground-var?
  "!! vars that cause outer joins"
  [sym-or-str]
  (.startsWith (extract-varname sym-or-str) "!!"))

(def ground-var? (complement unground-var?))

(defn vars2str [vars]
  (vec (map #(if (cascalog-var? %) (extract-varname %) %) vars)))

(defn- var-updater-fn [outfield?]
  (fn [[all equalities] v]
    (if (cascalog-var? v)
      (let [existing (get equalities v [])
            varlist  (if (or (empty? existing) (and outfield? (ground-var? v)))
                      (conj existing (str v (gen-unique-suffix)))
                      existing)
            newname  (if outfield? (last varlist) (first varlist))]
            [(conj all newname) (assoc equalities v varlist)] )
      [(conj all v) equalities] )))

(defn uniquify-vars [invars outvars equalities]
  (let [[invars equalities] (reduce (var-updater-fn false) [[] equalities] invars)
        [outvars equalities] (reduce (var-updater-fn true) [[] equalities] outvars)]
      [invars outvars equalities] ))

(defn mk-drift-map [vmap]
  (let [update-fn (fn [m [_ vals]]
                     (let [target (first vals)]
                       (reduce #(assoc %1 %2 target) m (rest vals))))]
      (reduce update-fn {} (seq vmap))))
