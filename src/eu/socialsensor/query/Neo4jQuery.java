package eu.socialsensor.query;

import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.Traversal;
import org.neo4j.tooling.GlobalGraphOperations;

import eu.socialsensor.benchmarks.FindShortestPathBenchmark;
import eu.socialsensor.graphdatabases.Neo4jGraphDatabase;

/**
 * Query implementation for Neo4j graph database
 * 
 * @author sotbeis
 * @email sotbeis@iti.gr
 */
@SuppressWarnings("deprecation")
public class Neo4jQuery implements Query {
	
	private GraphDatabaseService neo4jGraph = null;
		
	public static void main(String args[]) {
	}
		
	public Neo4jQuery(GraphDatabaseService neo4jGraph) {
		this.neo4jGraph = neo4jGraph;
	}
	
	@Override
	public void findNeighborsOfAllNodes() {
		try(Transaction tx = neo4jGraph.beginTx()) {
			for(Node n : GlobalGraphOperations.at(neo4jGraph).getAllNodes()) {
				for(Relationship relationship : n.getRelationships(Neo4jGraphDatabase.RelTypes.SIMILAR, Direction.OUTGOING)) {
					@SuppressWarnings("unused")
					Node neighbour = relationship.getOtherNode(n);
				}
			}
			tx.success();
		}
		
	}
	
	@Override
	public void findNodesOfAllEdges() {
		try(Transaction tx = neo4jGraph.beginTx()) {
			for(Relationship r : GlobalGraphOperations.at(neo4jGraph).getAllRelationships()) {
				@SuppressWarnings("unused")
				Node startNode = r.getStartNode();
				@SuppressWarnings("unused")
				Node endNode = r.getEndNode();
			}
		}
	}	
	
	@Override
	public void findShortestPaths() {
		try(Transaction tx = neo4jGraph.beginTx()) {
			PathFinder<Path> finder = GraphAlgoFactory.shortestPath(Traversal.expanderForTypes(Neo4jGraphDatabase.RelTypes.SIMILAR),20);
			Node n1 = neo4jGraph.findNodesByLabelAndProperty(Neo4jGraphDatabase.NODE_LABEL, "nodeId", "1").iterator().next();
			for(int i : FindShortestPathBenchmark.generatedNodes) {
				Node n2 = neo4jGraph.findNodesByLabelAndProperty(Neo4jGraphDatabase.NODE_LABEL, "nodeId", String.valueOf(i)).iterator().next();
				Path path = finder.findSinglePath(n1, n2);
				@SuppressWarnings("unused")
				int length = 0;
				if(path != null) {
					length = path.length();
				}
			}
			tx.success();
		}
	}

	
}
