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
import java.util.List;
import java.util.Map;
import java.util.Set;

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

    @Override
    public void initCommunityProperty() {

    }

    @Override
    public Set<Integer> getCommunitiesConnectedToNodeCommunities(int nodeCommunities) {
        return null;
    }

    @Override
    public Set<Integer> getNodesFromCommunity(int community) {
        return null;
    }

    @Override
    public Set<Integer> getNodesFromNodeCommunity(int nodeCommunity) {
        return null;
    }

    @Override
    public double getEdgesInsideCommunity(int nodeCommunity, int communityNodes) {
        return 0;
    }

    @Override
    public double getCommunityWeight(int community) {
        return 0;
    }

    @Override
    public double getNodeCommunityWeight(int nodeCommunity) {
        return 0;
    }

    @Override
    public void moveNode(int from, int to) {

    }

    @Override
    public int reInitializeCommunities() {
        return 0;
    }

    @Override
    public int getCommunityFromNode(int nodeId) {
        return 0;
    }

    @Override
    public int getCommunity(int nodeCommunity) {
        return 0;
    }

    @Override
    public int getCommunitySize(int community) {
        return 0;
    }

    @Override
    public Map<Integer, List<Integer>> mapCommunities(int numberOfCommunities) {
        return null;
    }
}
