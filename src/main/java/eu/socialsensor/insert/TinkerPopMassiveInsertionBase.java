package eu.socialsensor.insert;

import eu.socialsensor.graphdatabases.GraphDatabaseBase;
import eu.socialsensor.main.GraphDatabaseType;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import static eu.socialsensor.graphdatabases.GraphDatabaseBase.NODE_ID;

public abstract class TinkerPopMassiveInsertionBase extends InsertionBase<Vertex> {
    Map<String, Vertex> cache = new HashMap<>();
    Graph myGraph;
    public static final String NODE_LABEL = "Node";


    public TinkerPopMassiveInsertionBase(Graph graph, GraphDatabaseType type, File resultsPath) {
        super(type, resultsPath);
        myGraph = graph;
    }

    @Override
    protected Vertex getOrCreate(String value) {
        Vertex v = cache.get(value);
        if (v != null) {
            return v;
        } else {
            v = myGraph.addVertex(T.label, NODE_LABEL, NODE_ID, value);
            cache.put(value, v);
            return v;
        }
    }

    @Override
    protected void relateNodes(Vertex src, Vertex dest) {
        myGraph.traversal().V(src).addE(GraphDatabaseBase.SIMILAR).to(dest).iterate();
    }
}
