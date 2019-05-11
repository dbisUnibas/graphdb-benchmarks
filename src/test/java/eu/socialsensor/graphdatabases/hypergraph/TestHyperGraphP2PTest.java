package eu.socialsensor.graphdatabases.hypergraph;

import org.hypergraphdb.peer.HyperGraphPeer;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.Future;

public class TestHyperGraphP2PTest {

  @Test
  public void testBasicP2P(){
    File configFile = new File(this.getClass().getResource("/hgp2p.json").getPath());
    HyperGraphPeer peer = new HyperGraphPeer(configFile);
    Future<Boolean> startupResult = peer.start();
    try
    {
      if (startupResult.get())
      {
        System.out.println("Peer started successfully.");
      }
      else
      {
        System.out.println("Peer failed to start.");
        peer.getStartupFailedException().printStackTrace(System.err);
      }
    }
    catch (Exception e)
    {
      e.printStackTrace(System.err);
    }
  }
}
