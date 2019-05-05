package eu.socialsensor.graphdatabases.hypergraph.vertex;

import lombok.Getter;
import lombok.Setter;

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

}
