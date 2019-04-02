package eu.socialsensor.main;


import ch.unibas.dmi.dbis.chronos.agent.AbstractChronosAgent;
import ch.unibas.dmi.dbis.chronos.agent.ChronosJob;
import ch.unibas.dmi.dbis.chronos.agent.ExecutionException;
import eu.socialsensor.benchmarks.Benchmark;
import java.io.File;
import java.net.InetAddress;
import java.util.Map;
import java.util.Properties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class ChronosAgent extends AbstractChronosAgent {

    public static final Logger logger = LogManager.getLogger();


    ChronosAgent( InetAddress address, int port, boolean secure, boolean useHostname, String environment ) {
        super( address, port, secure, useHostname, environment );
    }


    @Override
    protected String[] getSupportedSystemNames() {
        return new String[]{ "graphbenchmarker" };
    }


    /**
     * @param inputDirectory Temporary input directory. Will be deleted by the environment after the (successful|failed|aborted) execution.
     * @param outputDirectory Temporary output directory. Will be deleted by the environment after the (successful|failed|aborted) execution.
     * @param results Key-Value results which are uploaded as json-file
     * @param prePhaseData Implementation specific data exchange object from the previous "yet unknown" phase -- typically null
     * @return Implementation specific data exchange object which is passed to the next (typically warmUp) phase parameter "prePhaseData"
     * @throws ExecutionException Can be thrown by the implementation; leads to the job state FAILED
     */
    @Override
    protected Object prepare( ChronosJob job, File inputDirectory, File outputDirectory, Properties results, Object prePhaseData ) throws ExecutionException {
        Map<String, String> settings = job.getParsedCdl();

        updateProgress( job, 1 );

        final BenchmarkConfiguration configuration = new BenchmarkConfiguration(settings);

        updateProgress( job, 5 );

        return configuration;
    }


    @Override
    protected Object warmUp( ChronosJob job, File inputDirectory, File outputDirectory, Properties results, Object prePhaseData ) throws ExecutionException {
        return prePhaseData == null ? new Object() : prePhaseData;
    }


    @Override
    protected Object execute( ChronosJob job, File inputDirectory, File outputDirectory, Properties results, Object prePhaseData ) throws ExecutionException {
        updateProgress( job, 6 );

        GraphDatabaseBenchmark benchmarks = new GraphDatabaseBenchmark( (BenchmarkConfiguration) prePhaseData );
        try {
            benchmarks.run();
        } catch ( Throwable t ) {
            logger.fatal( t.getMessage() );
        }

        final File runLog = new File( outputDirectory, "run.log" );

        updateProgress( job, 90 );
        return runLog;
    }


    @Override
    protected Object analyze( ChronosJob job, File inputDirectory, File outputDirectory, Properties results, Object prePhaseData ) throws ExecutionException {
        final File runLog = (File) prePhaseData;

        return prePhaseData == null ? new Object() : prePhaseData;
    }


    @Override
    protected Object clean( ChronosJob job, File inputDirectory, File outputDirectory, Properties results, Object prePhaseData ) throws ExecutionException {
        updateProgress( job, 95 );

        updateProgress( job, 100 );
        return prePhaseData == null ? new Object() : prePhaseData;
    }


    @Override
    protected void aborted( ChronosJob abortedJob ) {
        // TODO: abort running benchmark
    }


    @Override
    protected void failed( ChronosJob failedJob ) {
        // TODO: cleanup
    }


    public void updateProgress( ChronosJob job, int progress ) {
        setProgress( job, (byte) progress );
    }


}
