package eu.socialsensor.benchmarks;


import eu.socialsensor.graphdatabases.GraphDatabase;
import eu.socialsensor.main.BenchmarkConfiguration;
import eu.socialsensor.main.BenchmarkType;
import eu.socialsensor.main.GraphDatabaseType;
import eu.socialsensor.utils.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.util.List;


/**
 * HyperGraphDB
 * Distributed Single Insertion Benchmark
 *
 * @author Fabrizio Parrillo
 */
public class DistributedSingleInsertionBenchmark extends PermutingBenchmarkBase implements InsertsGraphData {

    public static final String DISTRIBUTED_INSERTION_TIMES_OUTPUT_FILE_NAME_BASE = "DISTRIBUTED_SINGLE_INSERTIONResults";
    private static final Logger LOG = LogManager.getLogger();


    public DistributedSingleInsertionBenchmark(BenchmarkConfiguration bench) {
        super(bench, BenchmarkType.DISTRIBUTED_SINGLE_INSERTION);
    }


    @Override
    public void post() {
        LOG.info("Write results to " + outputFile.getAbsolutePath());
        for (GraphDatabaseType type : bench.getSelectedDatabases()) {
            String prefix = outputFile.getParentFile().getAbsolutePath() + File.separator + DISTRIBUTED_INSERTION_TIMES_OUTPUT_FILE_NAME_BASE + "." + type.getShortname();
            List<List<Double>> insertionTimesOfEachScenario = Utils.getDocumentsAs2dList(prefix, bench.getScenarios());
            times.put(type, Utils.calculateMeanList(insertionTimesOfEachScenario));
            Utils.deleteMultipleFiles(prefix, bench.getScenarios());
        }
        // use the logic of the superclass method after populating the times map
        super.post();
    }


    @Override
    public void benchmarkOne(GraphDatabaseType type, int scenarioNumber) {
        GraphDatabase<?, ?, ?, ?> graphDatabase = Utils.createDatabaseInstance(bench, type);
        graphDatabase.createGraphForDistributedSingleLoad();
        graphDatabase.distributedSingleModeLoading(bench.getDataset(), bench.getResultsPath(), scenarioNumber);
        graphDatabase.shutdown();
    }
}
