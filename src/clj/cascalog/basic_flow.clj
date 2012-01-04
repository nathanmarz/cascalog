(ns cascalog.basic-flow
  (:require [cascalog.graph :as g]
            [cascalog.util :as u])
  (:import [java.util.concurrent Semaphore]
           [org.apache.log4j Logger]))

(defstruct basic-flow ::graph)
(defstruct basic-component ::id ::fn)
;; status can be :unstarted, :running, :successful, :failed
(defstruct component-state :status :thread :exception)

(defn mk-basic-flow []
  (struct basic-flow (g/mk-graph)))

(defn add-component!
  "Add a function to the flow with dependent components. Returns the new component."
  ([bflow func] (add-component! bflow func []))
  ([bflow func deps]
     (let [node (g/create-node (::graph bflow) (struct basic-component (u/uuid) func))]
       (doseq [d deps]
         (g/create-edge d node))
       node)))

(defn- mk-runner-fn [log sem state component]
  (fn []
    (try ((::fn component))
         (swap! state assoc :status :successful)
         (catch Throwable t
           (.error log "Component failed" t)
           (swap! state merge {:status :failed :exception t}))
         (finally (.release sem)))))

(defn- init-node-states [log bflow sem]
  (let [update-fn (fn [m node]
                    (let [state (atom (struct component-state
                                              :unstarted nil nil))
                          thread (Thread. (mk-runner-fn log sem state
                                                        (g/get-value node)))]
                      (swap! state assoc :thread thread)
                      (assoc m node state)))]
    (reduce update-fn {} (.vertexSet (::graph bflow)))))

(defn- fail-basic-flow! [log node-states]
  (let [states (map deref (vals node-states))
        running-states (filter #(= :running (:status %)) states)
        running-threads (map :thread running-states)]
    (.info log "Basic flow failed - interrupting components")
    (doseq [t running-threads] (.interrupt t))
    (.info log "Waiting for running components to finish")
    (doseq [t running-threads] (.join t))
    (throw (RuntimeException. "Basic flow failed"))))

(defn exec-basic-flow [bflow]
  (let [log (Logger/getLogger "basic-flow")
        sem (Semaphore. 0)
        node-states (init-node-states log bflow sem)]
    (loop []
      (doseq [[node state] (seq node-states)]
        (when (and (= :unstarted (:status @state))
                   (every? (fn [[_ s]] (= :successful (:status @s)))
                           (select-keys node-states (g/get-inbound-nodes node))))
          (swap! state assoc :status :running)
          (.start (:thread @state))))
      (.acquire sem)
      (let [statuses (map #(:status @%) (vals node-states))]
        (when (some (partial = :failed) statuses)
          (fail-basic-flow! log node-states))
        (when (some #{:running :unstarted} statuses)
          (recur))))
    (.info log "Basic flow completed successfully")))
