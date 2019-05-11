package eu.socialsensor.insert;

import eu.socialsensor.main.GraphDatabaseType;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.io.File;

public class TinkerPopMassiveInsertionNeo4j extends TinkerPopMassiveInsertionBase {
    public TinkerPopMassiveInsertionNeo4j(Graph graph, File resultsPath) {
        super(graph, GraphDatabaseType.TINKERPOP_NEO4J, resultsPath);
    }
}
