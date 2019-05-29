package eu.socialsensor.main;


import java.util.HashMap;
import java.util.Map;


/**
 * Enum containing constants that correspond to each database.
 *
 * @author Alexander Patrikalakis
 */
public enum GraphDatabaseType {
    ORIENT_DB( "OrientDB", null, "orient" ),
    NEO4J( "Neo4j", null, "neo4j" ),
    SPARKSEE( "Sparksee", null, "sparksee" ),
    TINKERPOP_NEO4J("gremlin-neo4j", null, "tinkerpop neo4j"),
    TINKERPOP_NEO4J_HA("gremlin-neo4j", null, "tinkerpop neo4j HACluster"),
    TINKERPOP_TINKERGRAPH("gremlin-tinkergraph", null, "tinkerpop tinkergraph");
    HYPERGRAPH_DB("HyperGraphDB", null, "HyperGraph");

    private final String backend;
    private final String api;
    private final String shortname;

    public static final Map<String, GraphDatabaseType> STRING_REP_MAP = new HashMap<>();


    static {
        for ( GraphDatabaseType db : values() ) {
            STRING_REP_MAP.put( db.getShortname(), db );
        }
    }


    GraphDatabaseType( String api, String backend, String shortname ) {
        this.api = api;
        this.backend = backend;
        this.shortname = shortname;
    }


    public String getBackend() {
        return backend;
    }


    public String getApi() {
        return api;
    }


    public String getShortname() {
        return shortname;
    }
}
