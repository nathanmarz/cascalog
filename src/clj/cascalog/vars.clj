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

(ns cascalog.vars)

;; TODO: better to use UUIDs to avoid name collisions with client code?
;; Are the size of fields an issue in the actual flow execution perf-wise?
(let [i (atom 0)]
  (defn gen-unique-suffix []
    (str "__gen" (swap! i inc))))

(defn- gen-var-fn [prefix]
  (fn this
    ([] (this ""))
    ([suffix] (str prefix (gen-unique-suffix) suffix))))

(def gen-non-nullable-var (gen-var-fn "?"))
(def gen-nullable-var (gen-var-fn "!"))
(def gen-ungrounding-var (gen-var-fn "!!"))

(defn gen-nullable-vars
"
  Generates the given number, 'amt', of nullable variables in a sequence.

  Example:
  (let [var-seq (gen-nullable-vars n)]
    (?<- (hfs-textline out-path)
         var-seq
         (in :>> var-seq)))
"
  ([amt] (gen-nullable-vars "" amt))
  ([suffix amt]
     (vec (take amt (repeatedly (partial gen-nullable-var suffix))))))

(defn gen-non-nullable-vars
"
  Generates the given number, 'amt', of non-nullable variables in a sequence.

  Example:
  (let [var-seq (gen-non-nullable-vars n)]
    (?<- (hfs-textline out-path)
         var-seq
         (in :>> var-seq)))
"  
  [amt]
  (vec (take amt (repeatedly gen-non-nullable-var))))

(defn- extract-varname
  ([v] (extract-varname v gen-nullable-var))
  ([v gen-var]
     (let [actname (if (symbol? v) (name v) v)]
       (if (= "_" actname) (gen-var) actname))))

(def cascalog-keyword? #{:> :< :<< :>> :fn> :#> :?})

(defn cascalog-var? 
"
  A predicate on 'obj' to check is it a cascalog variable.
"  
  [obj]
  (if (or (symbol? obj) (string? obj))
    (let [obj (extract-varname obj)]
      ((complement nil?) (some #(.startsWith obj %) ["?" "!" "!!"])))
    false))

(defn uniquify-var 
  "Return a modified variable name which is unique."  
  [v]
  (str v (gen-unique-suffix)))

(defn non-nullable-var? 
"
  ? vars that is non-nullable
"  
  [sym-or-str]
  (try (.startsWith (extract-varname sym-or-str) "?")
       (catch Exception e nil)))

(def ^{:doc "! or !! vars that are nullable."}
      nullable-var?
  (complement non-nullable-var?))

(defn unground-var?
  "!! vars that cause outer joins"
  [sym-or-str]
  (try (.startsWith (extract-varname sym-or-str) "!!")
       (catch Exception e nil)))

(def ^{:doc "? and ! vars that can cause joins."}
      ground-var? (complement unground-var?))

(defn- flatten-vars [vars]
  (flatten (map #(if (map? %) (seq %) %) vars)))

(defn- sanitize-elem [e anon-gen]
  (cond (cascalog-var? e) (extract-varname e anon-gen)
        (= (str e) "&") (str e) ; to support destructuring in predicate macros
        :else e))

(defn- sanitize-vec [v anon-gen] (vec (map sanitize-elem v (repeat anon-gen))))

(defn- sanitize-map [m anon-gen]
  (reduce (fn [ret k] (assoc ret k (sanitize-elem (m k) anon-gen)))
          {} (keys m)))

(defn sanitize-unknown [e anon-gen]
  (cond (map? e) (sanitize-map e anon-gen)
        (vector? e) (sanitize-vec e anon-gen)
        :else (sanitize-elem e anon-gen)))

(defn vars->str [vars]
  (let [anon-gen (if (some #(and (cascalog-var? %)
                                 (unground-var? %))
                           (flatten-vars vars))
                   gen-ungrounding-var
                   gen-nullable-var)]
    (vec (map sanitize-unknown vars
              (repeat anon-gen)))))

(defn- var-updater-fn [force-unique?]
  (fn [[all equalities] v]
    (if (cascalog-var? v)
      (let [existing (get equalities v [])
            varlist  (cond (empty? existing)  (conj existing v)
                           (and force-unique? (ground-var? v)) (conj existing (uniquify-var v))
                           :else               existing)
            newname  (if force-unique? (last varlist) (first varlist))]
        [(conj all newname) (assoc equalities v varlist)] )
      [(conj all v) equalities] )))

(defn uniquify-vars [vars force-unique? equalities]
  (let [[vars equalities] (reduce (var-updater-fn force-unique?) [[] equalities] vars)]
    [vars equalities] ))

(defn mk-drift-map [vmap]
  (let [update-fn (fn [m [_ vals]]
                    (let [target (first vals)]
                      (reduce #(assoc %1 %2 target) m (rest vals))))]
    (reduce update-fn {} (seq vmap))))
