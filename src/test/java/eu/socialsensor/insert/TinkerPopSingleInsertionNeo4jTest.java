package eu.socialsensor.insert;

import com.google.common.io.Files;
import eu.socialsensor.main.GraphDatabaseType;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jGraph;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.*;

public class TinkerPopSingleInsertionNeo4jTest {
    Graph graph;

    @Before
    public void setUp() throws Exception {
        Configuration config = new BaseConfiguration();
        config.addProperty(Neo4jGraph.CONFIG_DIRECTORY, Files.createTempDir().toString());
        graph = Neo4jGraph.open(config);
    }

    @Test
    public void getOrCreate() {
        assertEquals(0, (long) graph.traversal().V().count().next());
        InsertionBase<Vertex> ins =
                new TinkerPopSingleInsertionNeo4j(
                        graph, GraphDatabaseType.TINKERPOP_NEO4J, new File("."));
        TinkerPopInsertionTestCases.getOrCreate(graph, ins);
    }

    @Test
    public void relateNodes() {
        assertEquals(0, (long) graph.traversal().E().count().next());
        InsertionBase<Vertex> ins =
                new TinkerPopSingleInsertionNeo4j(
                        graph, GraphDatabaseType.TINKERPOP_NEO4J, new File("."));
        TinkerPopInsertionTestCases.relateNodes(graph, ins);
    }

}