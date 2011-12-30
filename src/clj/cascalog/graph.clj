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

(ns cascalog.graph
  (:use cascalog.util)
  (:import [org.jgrapht.graph DefaultDirectedGraph]
           [cascalog SimplePrintDirectedGraph]
           [org.jgrapht EdgeFactory]))

(defstruct edge :source :target ::extra-data)
(defstruct node ::graph ::value ::extra-data)

(defn get-extra-data [obj kw]
  (@(::extra-data obj) kw))

(defn add-extra-data [obj kw val]
  (swap! (::extra-data obj) assoc kw val))

(defn update-extra-data [obj kw afn]
  (swap! (::extra-data obj)
         (fn [curr]
           (assoc curr kw (afn (curr kw))))))

(defn mk-graph []
  (SimplePrintDirectedGraph.
   (proxy [EdgeFactory] []
     (createEdge [source target]
       (struct edge source target (atom {}))))))

(defn create-node [graph value]
  (let [ret (struct node graph value (atom {}))]
    (.addVertex graph ret)
    ret))

(defn create-edge [node1 node2]
  (.addEdge (::graph node1) node1 node2))

(defn connect-value
  "Creates a node for val and creates an edge from node -> new node. Returns new node"
  [node val]
  (let [n2 (create-node (::graph node) val)]
    (create-edge node n2)
    n2))

(defn get-graph [node]
  (::graph node))

(defn get-value [node]
  (::value node))

(defn get-outbound-edges [node]
  (or (seq (.outgoingEdgesOf (::graph node) node))
      []))

(defn get-inbound-edges [node]
  (or (seq (.incomingEdgesOf (::graph node) node))
      []))

(defn get-outbound-nodes [node]
  (map :target (get-outbound-edges node)))

(defn get-inbound-nodes [node]
  (map :source (get-inbound-edges node)))

