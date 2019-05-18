package eu.socialsensor.graphdatabases;

import eu.socialsensor.insert.TinkerPopMassiveInsertionBase;
import eu.socialsensor.insert.TinkerPopMassiveInsertionNeo4j;
import eu.socialsensor.insert.TinkerPopSingleInsertionBase;
import eu.socialsensor.insert.TinkerPopSingleInsertionNeo4j;
import eu.socialsensor.main.BenchmarkConfiguration;
import eu.socialsensor.main.BenchmarkingException;
import eu.socialsensor.main.GraphDatabaseType;
import eu.socialsensor.utils.Utils;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jGraph;

import java.io.File;

/**
 * Class implementing a tinkerpop-embedded Neo4j database
 *
 * @author Gabriel Zihlmann
 */
public class TinkerPopNeo4j extends TinkerPopBase {
    BenchmarkConfiguration config;
    GraphDatabaseType type;

    public TinkerPopNeo4j(BenchmarkConfiguration config, GraphDatabaseType type, File dbStorageDirectory) {
        super(type, dbStorageDirectory);
        this.config = config;
        this.type = type;
    }

    private Configuration getSingleConfig() {
        Configuration config = new BaseConfiguration();
        config.addProperty(Neo4jGraph.CONFIG_DIRECTORY, dbStorageDirectory.toString());
        return config;
    }

    private Configuration getHAConfig() {
        Configuration config = new BaseConfiguration();
        config.addProperty(Neo4jGraph.CONFIG_DIRECTORY, dbStorageDirectory.toString());
        config.addProperty(Neo4jGraph.CONFIG_CONF + ".ha.server_id", "1");
        config.addProperty(Neo4jGraph.CONFIG_CONF + ".ha.initial_hosts", "node1:5001\\,node2:5001\\,node3:5001");
        config.addProperty(Neo4jGraph.CONFIG_CONF + ".ha.host.coordination", "node1:5001");
        config.addProperty(Neo4jGraph.CONFIG_CONF + ".ha.host.data", "node1:6001");
        return config; //todo how to set the port of this server? Is it the coordination property?
        // need to manually set up other nodes...
    }

    // according to https://neo4j.com/docs/java-reference/current/tutorials-java-embedded/
    // it's not possible to run causal cluster in embedded mode...
    // will need to set up gremlin server that then connects to non-embedded Neo4j. How to do that??

    @Override
    public void open() {
        if (graph != null) {
            return; //already open
        }
        Configuration config;

        if (this.type == GraphDatabaseType.TINKERPOP_NEO4J_HA) {
            config = getHAConfig();
        } else {
            config = getSingleConfig();
        }

        graph = Neo4jGraph.open(config);
    }

    @Override
    public void createGraphForSingleLoad() {
//         purge before creating graph
        shutdown();
        delete();
        open();
        // we can only create indexes via cypher... or directly java API. Not gremlin
        createIndexes();
        graph.tx().commit();
    }

    private void createIndexes() {
        ((Neo4jGraph) graph).cypher(String.format("CREATE INDEX ON :%s(%s)",
                TinkerPopMassiveInsertionBase.NODE_LABEL, NODE_ID));
        ((Neo4jGraph) graph).cypher(String.format("CREATE INDEX ON :%s(%s)",
                TinkerPopMassiveInsertionBase.NODE_LABEL, COMMUNITY));
        ((Neo4jGraph) graph).cypher(String.format("CREATE INDEX ON :%s(%s)",
                TinkerPopMassiveInsertionBase.NODE_LABEL, NODE_COMMUNITY));
    }

    @Override
    public void massiveModeLoading(File dataPath) {
        TinkerPopMassiveInsertionNeo4j ins = new TinkerPopMassiveInsertionNeo4j(graph, type,null);
        ins.createGraph(dataPath, 0); // what's with these parameters? not needed here?
        graph.tx().commit();
    }

    @Override
    public void singleModeLoading(File dataPath, File resultsPath, int scenarioNumber) {
        TinkerPopSingleInsertionNeo4j ins = new TinkerPopSingleInsertionNeo4j(graph, type, resultsPath);
        ins.createGraph(dataPath, scenarioNumber);
        graph.tx().commit();
    }

    @Override
    public void createGraphForMassiveLoad() {
        // purge before creating graph
        shutdown();
        delete();
        open();
        createIndexes();
        graph.tx().commit();
    }

    @Override
    public void shutdown() {
        if (graph == null) {
            return;
        } else {

            try {
                graph.close();
            } catch (Exception e) {
                throw new BenchmarkingException( "unable to shutdown database", e );
            }
        }

    }

    @Override
    public void delete() {
        Utils.deleteRecursively( dbStorageDirectory );
    }

    @Override
    public void shutdownMassiveGraph() {
        shutdown();
    }
}
