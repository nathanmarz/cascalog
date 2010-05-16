(ns cascalog.basic-flow-test
  (:use [clojure test set])
  (:use [cascalog basic-flow])
  )

(defmacro throws? [eclass form]
  `(try
    ~form
    (is (= true false))
   (catch ~eclass e#)))

(defn- append-component [vec-atom val]
  (fn []
    (swap! vec-atom conj val)))

(defn- failing-component []
  (fn [] (throw (RuntimeException. "Fail!"))))

(deftest test-linear
  (let [vec-state (atom [])
        flow (mk-basic-flow)
        c1 (add-component! flow (append-component vec-state "a"))
        c2 (add-component! flow (append-component vec-state "b") [c1])
        c3 (add-component! flow (append-component vec-state "d") [c2])
        c4 (add-component! flow (append-component vec-state "c") [c3])
        c5 (add-component! flow (append-component vec-state "e") [c4])]
        (exec-basic-flow flow)
        (is (= ["a" "b" "d" "c" "e"] @vec-state))
        ))

(defn inc-component [state expected-val]
  (fn []
    (when-not (= expected-val @state)
      (throw (RuntimeException. (str "Did not get expected val " expected-val))))
    (swap! state inc)))

(defn inc-wait-comp [state wait-val]
  (fn []
    (swap! state inc)
    (loop []
      (Thread/sleep 100)
      (when-not (= wait-val @state)
        (recur)))
    ))

(deftest test-branch
  (let [state (atom 0)
        flow (mk-basic-flow)
        c1 (add-component! flow (inc-component state 0))
        c2 (add-component! flow (inc-component state 1) [c1])
        c3 (add-component! flow (inc-wait-comp state 4) [c2])
        c4 (add-component! flow (inc-wait-comp state 4) [c2])]
      (exec-basic-flow flow)
      (is (= 4 @state))
    ))

(defn deps-component [state val expected-set]
  (fn []
    (Thread/sleep 50)
    (swap! state conj val)
    (when-not (every? (partial contains? @state) expected-set)
      (throw (RuntimeException. (str "Did not contain " expected-set val))))
    ))

(deftest test-complex
  (let [state (atom #{})
        flow (mk-basic-flow)
        r1 (add-component! flow (deps-component state "r1" #{}))
        r2 (add-component! flow (deps-component state "r2" #{}))
        r3 (add-component! flow (deps-component state "r3" #{}))
        a (add-component! flow (deps-component state "a" #{"r1"}) [r1])
        b (add-component! flow (deps-component state "b" #{"r2" "r3"}) [r2 r3])
        c (add-component! flow (deps-component state "c" #{"r1"}) [r1])
        d (add-component! flow (deps-component state "d" #{"r1" "a"}) [a])
        e (add-component! flow (deps-component state "e" #{"r1" "c"}) [c])
        f (add-component! flow (deps-component state "f" #{"b" "r2" "r3" "r1" "c"}) [c b])]
        (exec-basic-flow flow)
        (is (= #{"r1" "r2" "r3" "a" "b" "c" "d" "e" "f"} @state))
    ))

(deftest test-error
  (let [state (atom #{})
        flow (mk-basic-flow)
        r1 (add-component! flow (deps-component state "r1" #{}))
        a (add-component! flow (deps-component state "a" #{"r1"}) [r1])
        b (add-component! flow (deps-component state "b" #{"a" "r1"}) [a])
        c (add-component! flow (failing-component) [b])
        d (add-component! flow (deps-component state "d" #{"b" "a" "r1"}) [c])
        ]
      (throws? Exception (exec-basic-flow flow))
      (is (= #{"r1" "a" "b"} @state))
    ))