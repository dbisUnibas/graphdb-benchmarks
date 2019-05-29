package eu.socialsensor.graphdatabases.hypergraph.vertex;

import lombok.Getter;
import lombok.Setter;

import java.util.Objects;

/**
 * Hyper Graph
 * Node: Node
 *
 * @author Fabrizio Parrillo
 */
public class Node {

    @Getter
    @Setter
    int id;

    @Getter
    @Setter
    int community;

    @Getter
    @Setter
    int nodeCommunity;

    public Node() {
    }  // nullary-constructor

    public Node(int id, int community, int nodeCommunity) {
        this.id = id;
        this.community = community;
        this.nodeCommunity = nodeCommunity;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Node node = (Node) o;
        return id == node.id &&
                community == node.community &&
                nodeCommunity == node.nodeCommunity;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, community, nodeCommunity);
    }
}
