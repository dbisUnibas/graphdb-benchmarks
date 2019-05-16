package eu.socialsensor.main;

import eu.socialsensor.graphdatabases.hypergraph.vertex.NodeQueries;
import org.hypergraphdb.peer.HyperGraphPeer;
import org.hypergraphdb.util.HGUtils;

import java.io.File;
import java.util.concurrent.Future;

public class P2P {

  public static void main(String[] args) throws InterruptedException {
    HyperGraphPeer peer;
    File config =  new File("/home/fp/Repositories/graphdb-benchmarks/src/main/resources/hgp2pA.json");
    peer = P2P.startPeer(config);
    NodeQueries.addIndex(peer.getGraph());
    while (peer.getConnectedPeers().isEmpty())
      Thread.sleep(500);

    System.out.println("Connected peers to " + peer.getConfiguration().at("interfaceConfig").at("user"));
    peer.getConnectedPeers().forEach(System.out::println);

    while (true)
      try {
        Thread.sleep(5000);
        peer.getGraph().runMaintenance();
      } catch (InterruptedException ex) { break; }

  }

  static HyperGraphPeer startPeer(File config) {
    HyperGraphPeer peer = new HyperGraphPeer(config);
    Future<Boolean> startupResult = peer.start();
    try {
      if (startupResult.get()) {
        System.out.println("Peer " + peer.getConfiguration().at("peerName") + " started successfully.");
      } else {
        System.out.println("Peer failed to start.");
        HGUtils.throwRuntimeException(peer.getStartupFailedException());
      }
    } catch (Exception e) {
      peer.stop();
      throw new RuntimeException(e);
    }
    return peer;
  }
}
