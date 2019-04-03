package eu.socialsensor.main;


import eu.socialsensor.benchmarks.Benchmark;
import eu.socialsensor.benchmarks.ClusteringBenchmark;
import eu.socialsensor.benchmarks.DeleteGraphBenchmark;
import eu.socialsensor.benchmarks.FindNeighboursOfAllNodesBenchmark;
import eu.socialsensor.benchmarks.FindNodesOfAllEdgesBenchmark;
import eu.socialsensor.benchmarks.FindShortestPathBenchmark;
import eu.socialsensor.benchmarks.MassiveInsertionBenchmark;
import eu.socialsensor.benchmarks.SingleInsertionBenchmark;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileDeleteStrategy;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URL;
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
    public static final String DEFAULT_INPUT_PROPERTIES = "META-INF/input.properties";
    private final BenchmarkConfiguration config;

    private final CsvReporter reporter;


    public static final Configuration getAppConfigFromClasspath() {
        Configuration appConfig;
        try {
            ClassLoader classLoader = GraphDatabaseBenchmark.class.getClassLoader();
            URL resource = classLoader.getResource( DEFAULT_INPUT_PROPERTIES );
            appConfig = new PropertiesConfiguration( resource );
        } catch ( ConfigurationException e ) {
            throw new IllegalArgumentException( String.format( "Unable to load properties file from classpath because %s", e.getMessage() ) );
        }
        return appConfig;
    }


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
        if ( config.publishGraphiteMetrics() ) {
            final Graphite graphite = new Graphite( new InetSocketAddress( config.getGraphiteHostname(), 80 /*port*/ ) );
            final GraphiteReporter reporter = GraphiteReporter.forRegistry( metrics )
                    .convertRatesTo( TimeUnit.SECONDS )
                    .convertDurationsTo( TimeUnit.MILLISECONDS )
                    .filter( MetricFilter.ALL )
                    .build( graphite );
            reporter.start( config.getGraphiteReportingInterval(), TimeUnit.MILLISECONDS );
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


    /**
     * This is the main function. Set the proper property file and run
     */
    public static void main( String[] args ) {
        final String inputPath = args.length != 1 ? null : args[0];

        final Configuration appConfig;
        try {
            appConfig =
                    inputPath == null
                            ? getAppConfigFromClasspath()
                            : new PropertiesConfiguration( new File( inputPath ) );
        } catch ( ConfigurationException e ) {
            throw new IllegalArgumentException( String.format( "Unable to load properties file %s because %s", inputPath, e.getMessage() ) );
        }

        GraphDatabaseBenchmark benchmarks = new GraphDatabaseBenchmark( new BenchmarkConfiguration( appConfig ) );
        try {
            benchmarks.run();
        } catch ( Throwable t ) {
            logger.fatal( t.getMessage() );
            System.exit( 1 );
        }
        System.exit( 0 );
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
