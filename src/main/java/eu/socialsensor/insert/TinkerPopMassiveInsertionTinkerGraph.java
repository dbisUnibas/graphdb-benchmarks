package eu.socialsensor.insert;

import eu.socialsensor.main.GraphDatabaseType;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.io.File;

public class TinkerPopMassiveInsertionTinkerGraph extends TinkerPopMassiveInsertionBase {
    public TinkerPopMassiveInsertionTinkerGraph(Graph graph, File resultsPath) {
        super(graph, GraphDatabaseType.TINKERPOP_TINKERGRAPH, resultsPath);
    }
}
