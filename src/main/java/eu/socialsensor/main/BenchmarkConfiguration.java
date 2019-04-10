package eu.socialsensor.main;


import com.google.common.primitives.Ints;
import eu.socialsensor.dataset.DatasetFactory;
import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import lombok.Getter;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.math3.util.CombinatoricsUtils;


/**
 * @author Alexander Patrikalakis
 */
public class BenchmarkConfiguration {

    // OrientDB Configuration
    private static final String LIGHTWEIGHT_EDGES = "lightweight-edges";

    // Sparksee / DEX configuration
    private static final String LICENSE_KEY = "license-key";

    // benchmark configuration
    private static final String DATASET = "dataset";
    private static final String DATABASE_STORAGE_DIRECTORY = "database-storage-directory";
    private static final String ACTUAL_COMMUNITIES = "actual-communities";
    private static final String NODES_COUNT = "nodes-count";
    private static final String RANDOMIZE_CLUSTERING = "randomize-clustering";
    private static final String CACHE_VALUES = "cache-values";
    private static final String CACHE_INCREMENT_FACTOR = "cache-increment-factor";
    private static final String CACHE_VALUES_COUNT = "cache-values-count";
    private static final String PERMUTE_BENCHMARKS = "permute-benchmarks";
    private static final String RANDOM_NODES = "shortest-path-random-nodes";

    private static final Set<String> metricsReporters = new HashSet<>();


    static {
        metricsReporters.add( "csv" );
    }


    @Getter private final File dataset;
    @Getter private final List<BenchmarkType> benchmarkTypes;
    @Getter private final SortedSet<GraphDatabaseType> selectedDatabases;
    @Getter private final File resultsPath;

    // storage directory
    @Getter private final File dbStorageDirectory;

    // metrics (optional)
    @Getter private final long csvReportingInterval; // Time between dumps of CSV files containing Metrics data, in milliseconds
    @Getter private final File csvDir; // Metrics CSV output directory

    // storage backend specific settings
    @Getter private final Boolean orientLightweightEdges; // Orient
    @Getter private final String sparkseeLicenseKey;  // Sparksee

    // shortest path
    @Getter private final int randomNodes;

    // clustering
    @Getter private final Boolean randomizedClustering;
    @Getter private final Integer nodesCount;
    @Getter private final Integer cacheValuesCount;
    @Getter private final Double cacheIncrementFactor;
    @Getter private final List<Integer> cacheValues;
    @Getter private final File actualCommunities;
    @Getter private final boolean permuteBenchmarks;
    @Getter private final int scenarios;



    public BenchmarkConfiguration( Configuration appconfig ) {
        if ( appconfig == null ) {
            throw new IllegalArgumentException( "appconfig may not be null" );
        }

        Configuration eu = appconfig.subset( "eu" );
        Configuration socialsensor = eu.subset( "socialsensor" );

        //metrics
        final Configuration metrics = socialsensor.subset( "metrics" );

        final Configuration csv = metrics.subset( "csv" );
        this.csvReportingInterval = metrics.getLong( "interval", 1000 /*ms*/ );
        this.csvDir = csv.containsKey( "directory" ) ? new File( csv.getString( "directory", System.getProperty( "user.dir" ) /*default*/ ) ) : null;

        Configuration orient = socialsensor.subset( "orient" );
        orientLightweightEdges = orient.containsKey( LIGHTWEIGHT_EDGES ) ? orient.getBoolean( LIGHTWEIGHT_EDGES ) : null;

        Configuration sparksee = socialsensor.subset( "sparksee" );
        sparkseeLicenseKey = sparksee.containsKey( LICENSE_KEY ) ? sparksee.getString( LICENSE_KEY ) : null;


        // database storage directory
        if ( !socialsensor.containsKey( DATABASE_STORAGE_DIRECTORY ) ) {
            throw new IllegalArgumentException( "configuration must specify database-storage-directory" );
        }
        dbStorageDirectory = new File( socialsensor.getString( DATABASE_STORAGE_DIRECTORY ) );
        dataset = validateReadableFile( socialsensor.getString( DATASET ), DATASET );

        // load the dataset
        DatasetFactory.getInstance().getDataset( dataset );

        if ( !socialsensor.containsKey( PERMUTE_BENCHMARKS ) ) {
            throw new IllegalArgumentException( "configuration must set permute-benchmarks to true or false" );
        }
        permuteBenchmarks = socialsensor.getBoolean( PERMUTE_BENCHMARKS );

        List<?> benchmarkList = socialsensor.getList( "benchmarks" );
        benchmarkTypes = new ArrayList<>();
        for ( Object str : benchmarkList ) {
            benchmarkTypes.add( BenchmarkType.valueOf( str.toString() ) );
        }

        selectedDatabases = new TreeSet<>();
        for ( Object database : socialsensor.getList( "databases" ) ) {
            if ( !GraphDatabaseType.STRING_REP_MAP.keySet().contains( database.toString() ) ) {
                throw new IllegalArgumentException( String.format( "selected database %s not supported", database.toString() ) );
            }
            selectedDatabases.add( GraphDatabaseType.STRING_REP_MAP.get( database ) );
        }
        scenarios = permuteBenchmarks ? Ints.checkedCast( CombinatoricsUtils.factorial( selectedDatabases.size() ) ) : 1;

        resultsPath = new File( System.getProperty( "user.dir" ), socialsensor.getString( "results-path" ) );
        if ( !resultsPath.exists() && !resultsPath.mkdirs() ) {
            throw new IllegalArgumentException( "unable to create results directory" );
        }
        if ( !resultsPath.canWrite() ) {
            throw new IllegalArgumentException( "unable to write to results directory" );
        }

        randomNodes = socialsensor.getInteger( RANDOM_NODES, 100 );

        if ( this.benchmarkTypes.contains( BenchmarkType.CLUSTERING ) ) {
            if ( !socialsensor.containsKey( NODES_COUNT ) ) {
                throw new IllegalArgumentException( "the CW benchmark requires nodes-count integer in config" );
            }
            nodesCount = socialsensor.getInt( NODES_COUNT );

            if ( !socialsensor.containsKey( RANDOMIZE_CLUSTERING ) ) {
                throw new IllegalArgumentException( "the CW benchmark requires randomize-clustering bool in config" );
            }
            randomizedClustering = socialsensor.getBoolean( RANDOMIZE_CLUSTERING );

            if ( !socialsensor.containsKey( ACTUAL_COMMUNITIES ) ) {
                throw new IllegalArgumentException( "the CW benchmark requires a file with actual communities" );
            }
            actualCommunities = validateReadableFile( socialsensor.getString( ACTUAL_COMMUNITIES ), ACTUAL_COMMUNITIES );

            final boolean notGenerating = socialsensor.containsKey( CACHE_VALUES );
            if ( notGenerating ) {
                List<?> objects = socialsensor.getList( CACHE_VALUES );
                cacheValues = new ArrayList<>( objects.size() );
                cacheValuesCount = null;
                cacheIncrementFactor = null;
                for ( Object o : objects ) {
                    cacheValues.add( Integer.valueOf( o.toString() ) );
                }
            } else if ( socialsensor.containsKey( CACHE_VALUES_COUNT ) && socialsensor.containsKey( CACHE_INCREMENT_FACTOR ) ) {
                cacheValues = null;
                // generate the cache values with parameters
                if ( !socialsensor.containsKey( CACHE_VALUES_COUNT ) ) {
                    throw new IllegalArgumentException( "the CW benchmark requires cache-values-count int in config when cache-values not specified" );
                }
                cacheValuesCount = socialsensor.getInt( CACHE_VALUES_COUNT );

                if ( !socialsensor.containsKey( CACHE_INCREMENT_FACTOR ) ) {
                    throw new IllegalArgumentException( "the CW benchmark requires cache-increment-factor int in config when cache-values not specified" );
                }
                cacheIncrementFactor = socialsensor.getDouble( CACHE_INCREMENT_FACTOR );
            } else {
                throw new IllegalArgumentException( "when doing CW benchmark, must provide cache-values or parameters to generate them" );
            }
        } else {
            randomizedClustering = null;
            nodesCount = null;
            cacheValuesCount = null;
            cacheIncrementFactor = null;
            cacheValues = null;
            actualCommunities = null;
        }
    }


    // Chronos
    public BenchmarkConfiguration( Map<String, String> settings, File outputDirectory ) {

        // ---- Static values ----
        // Database dir
        dbStorageDirectory = new File( "storage" );

        // Results
        resultsPath = new File( outputDirectory, "results" );
        if ( !resultsPath.exists() && !resultsPath.mkdirs() ) {
            throw new IllegalArgumentException( "unable to create results directory" );
        }
        if ( !resultsPath.canWrite() ) {
            throw new IllegalArgumentException( "unable to write to results directory" );
        }

        // Csv dir
        this.csvDir = new File( outputDirectory, "metrics" );
        if ( !csvDir.exists() && !csvDir.mkdirs() ) {
            throw new IllegalArgumentException( "unable to create csv directory" );
        }
        if ( !csvDir.canWrite() ) {
            throw new IllegalArgumentException( "unable to write to csv directory" );
        }

        // permute benchmarks
        permuteBenchmarks = false;

        // For FindShortestPath workload, number of nodes for which to calculate shortest path
        //randomNodes = Integer.parseInt( settings.get( "shortestPathRandomNodes" ) );
        randomNodes = 100;

        // ---- Settings from Chronos ----

        benchmarkTypes = new ArrayList<>();
        if (settings.get("type").equals( "real" )) {
            // Benchmark Types
            benchmarkTypes.add( BenchmarkType.valueOf( settings.get( "insertionWorkload" ) ) );
            benchmarkTypes.add( BenchmarkType.valueOf( settings.get( "queryWorkload" ) ) );

            // Dataset
            dataset = validateReadableFile( "data/" + settings.get( "dataset" ) + ".txt", DATASET );
            DatasetFactory.getInstance().getDataset( dataset );

            randomizedClustering = null;
            nodesCount = null;
            cacheValuesCount = null;
            cacheIncrementFactor = null;
            cacheValues = null;
            actualCommunities = null;

        } else {
            // Workload
            benchmarkTypes.add( BenchmarkType.CLUSTERING );

            // Dataset
            dataset = validateReadableFile( "data/data/network1000.dat", DATASET );
            actualCommunities = validateReadableFile( "data/data/community1000.dat", ACTUAL_COMMUNITIES );

            randomizedClustering = Boolean.parseBoolean( settings.get("randomizeClustering" ) );
            nodesCount = Integer.parseInt( settings.get("nodesCount" ) );

            final boolean generateCacheValue = settings.containsKey( "generateCacheValue" ) && Boolean.parseBoolean( settings.get( "generateCacheValue" ) );
            if ( !generateCacheValue ) {
                cacheValues = new ArrayList<>( 1 );
                cacheValues.add( Integer.parseInt( settings.get("cacheValue" ) ) );
                cacheValuesCount = null;
                cacheIncrementFactor = null;
            } else {
                cacheValues = null;
                cacheValuesCount =  Integer.parseInt( settings.get("cacheValuesCount") );
                cacheIncrementFactor = Double.parseDouble( settings.get("cacheIncrementFactor") );
            }
        }

        // Database System
        if ( !GraphDatabaseType.STRING_REP_MAP.keySet().contains( settings.get( "system") ) ) {
            throw new IllegalArgumentException( String.format( "selected database %s not supported", settings.get( "system") ) );
        }
        selectedDatabases = new TreeSet<>();
        selectedDatabases.add( GraphDatabaseType.STRING_REP_MAP.get( settings.get( "system") ) );
        scenarios = permuteBenchmarks ? Ints.checkedCast( CombinatoricsUtils.factorial( selectedDatabases.size() ) ) : 1;

        // Orient
        orientLightweightEdges = settings.containsKey( "orientdb-lightweightEdges" ) ? Boolean.parseBoolean( settings.get( "orientdb.lightweightEdges" ) ) : null;

        // Sparksee
        sparkseeLicenseKey = settings.getOrDefault( "sparksee.licenseKey", null );

        // Metrics
        this.csvReportingInterval = settings.containsKey("csvReportingInterval") ? Long.parseLong( settings.get("csvReportingInterval")) : 1000;

    }


    private static File validateReadableFile( String fileName, String fileType ) {
        File file = new File( fileName );
        if ( !file.exists() ) {
            throw new IllegalArgumentException( String.format( "the %s does not exist: " + fileName, fileType ) );
        }

        if ( !(file.isFile() && file.canRead()) ) {
            throw new IllegalArgumentException( String.format( "the %s must be a file that this user can read: " + fileName, fileType ) );
        }
        return file;
    }



    public boolean publishCsvMetrics() {
        return csvDir != null;
    }

}
