package eu.socialsensor.main;


import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.MetricRegistry;
import eu.socialsensor.benchmarks.*;
import org.apache.commons.io.FileDeleteStrategy;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Locale;
import java.util.concurrent.TimeUnit;


/**
 * Main class for the execution of GraphDatabaseBenchmark.
 *
 * @author sotbeis, sotbeis@iti.gr
 * @author Alexander Patrikalakis
 */
public class GraphDatabaseBenchmark {

    public static final Logger logger = LogManager.getLogger();
    public static final MetricRegistry metrics = new MetricRegistry();
    private final BenchmarkConfiguration config;

    private final CsvReporter reporter;


    public GraphDatabaseBenchmark( BenchmarkConfiguration benchmarkConfiguration ) throws IllegalArgumentException {
        config = benchmarkConfiguration;
        if ( config.publishCsvMetrics() ) {
            reporter = CsvReporter.forRegistry( metrics )
                    .formatFor( Locale.US )
                    .convertRatesTo( TimeUnit.SECONDS )
                    .convertDurationsTo( TimeUnit.MILLISECONDS )
                    .build( config.getCsvDir() );
            reporter.start( config.getCsvReportingInterval(), TimeUnit.MILLISECONDS );
        } else {
            reporter = null;
        }
    }


    public void run() {
        //MetricRegistry registry = MetricRegistry.name(klass, names)
        for ( BenchmarkType type : config.getBenchmarkTypes() ) {
            runBenchmark( type );
        }
        stopCsvRecorder();
    }


    private final void runBenchmark( BenchmarkType type ) {
        final Benchmark benchmark;
        logger.info( type.longname() + " benchmark selected" );
        switch ( type ) {
            case MASSIVE_INSERTION:
                benchmark = new MassiveInsertionBenchmark( config );
                break;
            case SINGLE_INSERTION:
                benchmark = new SingleInsertionBenchmark( config );
                break;

            case DISTRIBUTED_SINGLE_INSERTION:
                benchmark = new DistributedSingleInsertionBenchmark( config );
                break;

            case FIND_ADJACENT_NODES:
                benchmark = new FindNodesOfAllEdgesBenchmark( config );
                break;
            case CLUSTERING:
                benchmark = new ClusteringBenchmark( config );
                break;
            case FIND_NEIGHBOURS:
                benchmark = new FindNeighboursOfAllNodesBenchmark( config );
                break;
            case FIND_SHORTEST_PATH:
                benchmark = new FindShortestPathBenchmark( config );
                break;
            case DELETION:
                benchmark = new DeleteGraphBenchmark( config );
                break;
            default:
                throw new UnsupportedOperationException( "unsupported benchmark " + type == null ? "null" : type.toString() );
        }
        benchmark.startBenchmark();
    }


    public void stopCsvRecorder() {
        reporter.stop();
    }


    public void cleanup() {
        try {
            FileDeleteStrategy.FORCE.delete( config.getDbStorageDirectory() );
        } catch ( IOException e ) {
            logger.fatal( "Unable to clean up db storage directory: " + e.getMessage() );
        }
    }
}
