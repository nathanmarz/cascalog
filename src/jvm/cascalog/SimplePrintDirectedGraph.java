package cascalog;

public class SimplePrintDirectedGraph extends org.jgrapht.graph.DefaultDirectedGraph {
    public SimplePrintDirectedGraph(org.jgrapht.EdgeFactory ef) {
        super(ef);
    }
    
    public String toString() {
        return "Graph";
    }
}