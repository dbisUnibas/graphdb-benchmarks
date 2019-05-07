package eu.socialsensor.insert;


import eu.socialsensor.main.GraphDatabaseType;
import org.hypergraphdb.HyperGraph;


/**
 * Implementation of massive Insertion in Neo4j graph database
 *
 * @author sotbeis, sotbeis@iti.gr
 * @author Alexander Patrikalakis
 */
public final class HyperGraphMassiveInsertion extends InsertionBase<Long> {

    public HyperGraphMassiveInsertion( HyperGraph inserter ) {
        super( GraphDatabaseType.HYPERGRAPH_DB, null /* resultsPath */ );
    }


    @Override
    protected Long getOrCreate( String value ) {
        return null;
    }


    @Override
    protected void relateNodes( Long src, Long dest ) {
    }
}
