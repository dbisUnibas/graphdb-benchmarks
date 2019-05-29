package eu.socialsensor.main;

import org.neo4j.graphdb.factory.HighlyAvailableGraphDatabaseFactory;

import java.io.File;

/*
just a class to be run from the jar that instantiates a neo4j HA node
very quick and even dirtier than it is quick...
 */
public class HANode {
    private HANode(String configFile) {
        File dir = new File(".", "n4j");

        new HighlyAvailableGraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder(dir)
                .loadPropertiesFromFile(configFile).newGraphDatabase();

    }

    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Must indicate config file as argument");
            System.exit(1);
        }
        HANode n = new HANode(args[0]);
    }
}
