(ns cascalog.vars)

(defn- extract-varname [v]
  (if (symbol? v) (name v) v))

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

(defn vars2str [vars]
  (vec (map #(if (cascalog-var? %) (extract-varname %) %) vars)))

;; TODO: better to use UUIDs to avoid name collisions with client code?
;; Are the size of fields an issue in the actual flow execution perf-wise?
(let [i (atom 0)]
  (defn gen-unique-suffix [] (str "__gen" (swap! i inc))))

(defn gen-nullable-var [] (str "!" (gen-unique-suffix)))

(defn uniquify-vars
  "Uniques the cascalog vars, same vars still have the same name"
  [vars equalities]
  (reduce (fn [[p m] v]
            (if (cascalog-var? v)
              (let [newname (if-let [existing (m v)]
                existing (str v (gen-unique-suffix)))]
                [(conj p newname) (assoc m v newname)])
              [(conj p v) m]))
    [[] equalities] vars))

; (defn uniquify-vars
;   "Uniques the cascalog vars, equal vars get put into same set in map"
;   [vars equalities]
;   (reduce (fn [[p m] v]
;             (if (cascalog-var? v)
;               (let [newv (str v (gen-unique-suffix))]
;                 [(conj p newv) (assoc m v (conj (get m v #{}) newv))])
;               [(conj p v) m] ))
;     [[] equalities] vars))

;; Operations on variable equivalences

; (defstruct var-index :var2super :super2vars)
; (defn mk-var-index [equality-map]
;   (let [update-fn (fn [m [k vals]]
;                     (reduce #(assoc %1 %2 k) m vals))
;         reversemap (reduce update-fn {} equality-map)]
;     (struct var-index reversemap equality-map)))
; 
; (defn- get-super-var [index v]
;   ((:var2super index) v))
; 
; (defn var-equiv-set [index v]
;   ((get-super-var index v) (:super2vars index)))
; 
; (defn remove-indexed-var [index v]
;   (let [super (get-super-var index v)
;         newvar2super (dissoc (:var2super index) v)
;         newsuper2vars (assoc (:super2vars index) super (disj (var-equiv-set index v) v))]
;     (struct var-index newvar2super newsuper2vars)))