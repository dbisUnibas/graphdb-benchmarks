package eu.socialsensor.graphdatabases;

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

    public TinkerPopNeo4j(BenchmarkConfiguration config, File dbStorageDirectory) {
        super(GraphDatabaseType.TINKERPOP_NEO4J, dbStorageDirectory);
        this.config = config;
    }

    @Override
    public void open() {
        if (graph != null) {
            return; //already open
        }
        Configuration config = new BaseConfiguration();
        config.addProperty(Neo4jGraph.CONFIG_DIRECTORY, dbStorageDirectory.toString());
        graph = Neo4jGraph.open(config);
    }

    @Override
    public void createGraphForSingleLoad() {
//         purge before creating graph
        shutdown();
        delete();
        open();
        // we can only create indexes via cypher... or directly java API. Not gremlin
        ((Neo4jGraph) graph).cypher(String.format("CREATE INDEX ON :%s(%s)", TinkerPopSingleInsertionBase.NODE_LABEL, NODE_ID));
        graph.tx().commit();
    }

    @Override
    public void massiveModeLoading(File dataPath) {
        TinkerPopMassiveInsertionNeo4j ins = new TinkerPopMassiveInsertionNeo4j(graph, null);
        ins.createGraph(dataPath, 0); // what's with these parameters? not needed here?
        graph.tx().commit();
    }

    @Override
    public void singleModeLoading(File dataPath, File resultsPath, int scenarioNumber) {
        TinkerPopSingleInsertionNeo4j ins = new TinkerPopSingleInsertionNeo4j(graph, resultsPath);
        ins.createGraph(dataPath, scenarioNumber);
        graph.tx().commit();
    }

    @Override
    public void createGraphForMassiveLoad() {
        // purge before creating graph
        shutdown();
        delete();
        open();
        ((Neo4jGraph) graph).cypher(String.format("CREATE INDEX ON :%s(%s)", TinkerPopSingleInsertionBase.NODE_LABEL, NODE_ID));
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
        System.out.println("Deleting DB");
        Utils.deleteRecursively( dbStorageDirectory );
    }

    @Override
    public void shutdownMassiveGraph() {
        shutdown();
    }
}
