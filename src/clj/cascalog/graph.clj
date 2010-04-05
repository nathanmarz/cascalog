(ns cascalog.graph
  (:import [org.jgrapht.graph DefaultDirectedGraph])
  (:import [org.jgrapht EdgeFactory])
  )

(defstruct edge :source :target ::extra-data)
(defstruct node ::graph ::value ::extra-data)

(defn get-extra-data [obj kw]
  (@(::extra-data obj) kw))

(defn add-extra-data [obj kw val]
  (swap! (::extra-data obj) assoc kw val))

(defn update-extra-data [obj kw afn]
  (swap! (::extra-data obj) (fn [curr]
    (assoc curr kw (afn (curr kw))))))

(defn mk-graph []
  (DefaultDirectedGraph.
    (proxy [EdgeFactory] []
      (createEdge [source target]
        (struct edge source target (atom {}))))))

(defn create-node [#^DefaultDirectedGraph graph value]
  (let [ret (struct node graph value (atom {}))]
    (.addVertex graph ret)
    ret ))

(defn create-edge [node1 node2]
  (.addEdge (::graph node1) node1 node2))

(defn get-value [node]
  (::value node))

(defn get-outbound-edges [node]
  (if-let [s (seq (.outgoingEdgesOf (::graph node) node))] s []))

(defn get-outbound-nodes [node]
  (map :target (get-outbound-edges node)))

