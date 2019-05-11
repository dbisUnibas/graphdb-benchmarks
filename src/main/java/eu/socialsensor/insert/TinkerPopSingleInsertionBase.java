package eu.socialsensor.insert;

import eu.socialsensor.graphdatabases.GraphDatabaseBase;
import eu.socialsensor.main.GraphDatabaseType;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.io.File;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.addV;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.unfold;

public abstract class TinkerPopSingleInsertionBase extends InsertionBase<Vertex> {
    private Graph myGraph;
    public static final String NODE_LABEL = "Node";
    boolean supportsTransactions;

    TinkerPopSingleInsertionBase(Graph graph, GraphDatabaseType type, File resultsPath) {
        super(type, resultsPath);
        myGraph = graph;
        supportsTransactions = myGraph.features().graph().supportsTransactions();
    }

    @Override
    protected Vertex getOrCreate(String value) {
        Vertex v = myGraph.traversal().V().has(NODE_LABEL, GraphDatabaseBase.NODE_ID, value).fold().coalesce(
                unfold(),
                addV(NODE_LABEL).property(GraphDatabaseBase.NODE_ID, value)).next();
        if (supportsTransactions) {
            myGraph.tx().commit();
        }
        return v;
    }

    @Override
    protected void relateNodes(Vertex src, Vertex dest) {
        // although we don't need the created edge, we need to use .iterate() to actually perform the traversal and
        // thus create the edge
        myGraph.traversal().V(src).addE(GraphDatabaseBase.SIMILAR).to(dest).iterate();
        if (supportsTransactions) {
            myGraph.tx().commit();
        }    }
}
