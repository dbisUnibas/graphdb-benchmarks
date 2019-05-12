package eu.socialsensor.graphdatabases.hypergraph;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.peer.HGPeerIdentity;
import org.hypergraphdb.peer.HyperGraphPeer;
import org.hypergraphdb.peer.cact.DefineAtom;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.Future;

public class TestHyperGraphP2PTest {

  @Test
  public void testBasicP2P(){
    // Let's startup our two peers.
    HyperGraphPeer peer1 = startPeer(new File(this.getClass().getResource("/hgp2pA.json").getPath()));
    HyperGraphPeer peer2 = startPeer(new File(this.getClass().getResource("/hgp2pB.json").getPath()));

    // Add some atom to the graph of the first peer.
    HGHandle fromPeer1 = peer1.getGraph().add("From Peer1");

    // Have our first peer initiate a "define-atom" activity which
    // will trigger a "HyperGraph.define" operation with the specified
    // atom at the other peer.
    //
    // The DefineAtom constructor takes the initiating peer as its
    // first argument, the handle of the atom to be send as its second
    // argument and the identity of the receiving peer.
    //
    // The the newly constructed activity is passed onto the ActivityManager's
    // initiate method which will take it from there.


    HGPeerIdentity pid2 = peer2.getIdentity();
    HyperGraph graph = peer2.getGraph();

    pid2.setIpAddress("127.0.0.1");

    peer1.getActivityManager().initiateActivity(
            new DefineAtom(peer1, fromPeer1, pid2)
    );

    // 2 seconds should be enough in a single machine to transfer the atom
    try { Thread.sleep(2000); } catch (Throwable t) { }

    // Let's check that the atom was properly transferred.
    String received = peer2.getGraph().get(fromPeer1);
    if (received != null)
      System.out.println("Peer 2 received " + received);
    else
      System.out.println("Peer 2 failed to receive anything.");
  }


  // Start a peer out of a given configuration file. This will have as a side
// effect to open the underlying database if not already opened, execute
// all bootstrapping operations found in the configuration and connect to
// the network.
  private static HyperGraphPeer startPeer(File configFile) {
    HyperGraphPeer peer = new HyperGraphPeer(configFile);
    Future<Boolean> startupResult = peer.start();
    try {
      if (startupResult.get()) {
        System.out.println("Peer started successfully.");
        return peer;
      } else {
        System.out.println("Peer failed to start.");
        peer.getStartupFailedException().printStackTrace(System.err);
      }
    } catch (Exception e) {
      e.printStackTrace(System.err);
    }
    return  null;
  }
}