package cascalog;

public class SimplePrintDirectedGraph extends org.jgrapht.graph.DefaultDirectedGraph {
    public SimplePrintDirectedGraph(org.jgrapht.EdgeFactory ef) {
        super(ef);
    }
    
    public String toString() {
        return "Graph"; //this avoids infinite printing issue in Clojure node -> graph -> node -> ...
    }
}