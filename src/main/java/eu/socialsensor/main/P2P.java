package eu.socialsensor.main;

import eu.socialsensor.graphdatabases.hypergraph.vertex.Node;
import eu.socialsensor.graphdatabases.hypergraph.vertex.NodeQueries;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.peer.HyperGraphPeer;
import org.hypergraphdb.peer.cact.DefineAtom;
import org.hypergraphdb.peer.cact.QueryCount;
import org.hypergraphdb.util.HGUtils;

import java.io.File;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class P2P {

  public static void main(String[] args) throws InterruptedException {
    HyperGraphPeer peer;
    File config =  new File("/home/fp/Repositories/graphdb-benchmarks/src/main/resources/hgp2pB.json");
    peer = P2P.startPeer(config);

    while (peer.getConnectedPeers().isEmpty())
      Thread.sleep(500);

    System.out.println("Connected peers to " + peer.getConfiguration().at("interfaceConfig").at("user"));
    peer.getConnectedPeers().forEach(System.out::println);
    Node n = new Node(1, 1,1);
    HGHandle testHandle = peer.getGraph().add(n);

    peer.getConnectedPeers().forEach(id -> peer.getActivityManager().initiateActivity(
            new DefineAtom(peer, testHandle, id),
            result -> {
              System.out.println("Activity " + result.getActivity().getId() + " finished.");

              if (result.getException() != null)
                System.out.println("With exception: " + result.getException());
            }));


    // 2 seconds should be enough in a single machine to transfer the atom
    try { Thread.sleep(2000); } catch (Throwable t) { }

    peer.getConnectedPeers().forEach(id ->
            peer.getActivityManager().initiateActivity(
                    new QueryCount(peer, NodeQueries.nodeType(), id),
                    result -> {
              System.out.println("Activity " + result.getActivity().getId() + " finished.");
                      System.out.println(((QueryCount)result.getActivity()).getResult());
              if (result.getException() != null)
                System.out.println("With exception: " + result.getException());
            }));

    Long count =  peer.getConnectedPeers().stream().map(id -> peer.getActivityManager().initiateActivity(new QueryCount(peer, NodeQueries.nodeType(), id)))
            .map(activityResultFuture -> {
                try {
                    return activityResultFuture.get();
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
                return null;
            }).filter(Objects::nonNull)
            .map(a -> (QueryCount) a.getActivity())
            .map(QueryCount::getResult).reduce((a, b) -> a + b).get();

    System.out.println(count);
    Thread.sleep(5000);


    while (true)
      try { Thread.sleep(5000); } catch (InterruptedException ex) { break; }

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
