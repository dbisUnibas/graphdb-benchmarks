package eu.socialsensor.graphdatabases;

import eu.socialsensor.insert.TinkerPopMassiveInsertionTinkerGraph;
import eu.socialsensor.insert.TinkerPopSingleInsertionTinkerGraph;
import eu.socialsensor.main.BenchmarkConfiguration;
import eu.socialsensor.main.BenchmarkingException;
import eu.socialsensor.main.GraphDatabaseType;
import eu.socialsensor.utils.Utils;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import java.io.File;

public class TinkerPopTinkerGraph extends TinkerPopBase {

    public TinkerPopTinkerGraph(BenchmarkConfiguration config, File dbStorageDirectory) {
        super(GraphDatabaseType.TINKERPOP_TINKERGRAPH, dbStorageDirectory);
    }


    @Override
    public void open() {
        Configuration conf = new BaseConfiguration();
        conf.addProperty(TinkerGraph.GREMLIN_TINKERGRAPH_GRAPH_LOCATION,
                new File(dbStorageDirectory, "tinkerGraph.gryo").toString());
        conf.addProperty(TinkerGraph.GREMLIN_TINKERGRAPH_GRAPH_FORMAT, "gryo"); // we need to persist to be able
        this.graph = TinkerGraph.open(conf); // to use data from insertion in later benchmarks
    }

    @Override
    public void createGraphForSingleLoad() {
        // delete before starting, otherwise this benchmark makes no sense, as data will already be loaded.
        // apparently framework doesn't do this? huh?
        delete();
        open();
        createIndexes();
    }

    private void createIndexes() {
        ((TinkerGraph) graph).createIndex(NODE_ID, Vertex.class); // index is essential for insert speed!
        ((TinkerGraph) graph).createIndex(COMMUNITY, Vertex.class);
        ((TinkerGraph) graph).createIndex(NODE_COMMUNITY, Vertex.class);
    }

    @Override
    public void massiveModeLoading(File dataPath) {
        TinkerPopMassiveInsertionTinkerGraph ins = new TinkerPopMassiveInsertionTinkerGraph(this.graph, null);
        ins.createGraph(dataPath, 0); //what's with these parameters?? NEO4j impl sets them 0/null...

    }

    @Override
    public void singleModeLoading(File dataPath, File resultsPath, int scenarioNumber) {
        TinkerPopSingleInsertionTinkerGraph ins = new TinkerPopSingleInsertionTinkerGraph(this.graph, resultsPath);
        ins.createGraph(dataPath, scenarioNumber);
    }

    @Override
    public void createGraphForMassiveLoad() {
        // delete before starting, otherwise this benchmark makes no sense, as data will already be loaded.
        // apparently framework doesn't do this? huh?
        delete();
        open();
        createIndexes();
    }

    @Override
    public void shutdown() { // persist DB
        try {
            graph.close();
        } catch (Exception e) {
            throw new BenchmarkingException(String.format("Can't close TinkerGraph! %s", e.getMessage()));
        }
    }

    @Override
    public void delete() {
        System.out.println("Deleting DB");
        Utils.deleteRecursively(dbStorageDirectory);
    }

    @Override
    public void shutdownMassiveGraph() {
        shutdown();
    }
}
