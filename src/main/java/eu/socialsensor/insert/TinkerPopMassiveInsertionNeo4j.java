package eu.socialsensor.insert;

import eu.socialsensor.main.GraphDatabaseType;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.io.File;

public class TinkerPopMassiveInsertionNeo4j extends TinkerPopMassiveInsertionBase {
    public TinkerPopMassiveInsertionNeo4j(Graph graph, GraphDatabaseType type, File resultsPath) {
        super(graph, type, resultsPath);
    }
}
