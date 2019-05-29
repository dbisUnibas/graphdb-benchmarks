package eu.socialsensor.main;

import eu.socialsensor.graphdatabases.hypergraph.vertex.NodeQueries;
import org.hypergraphdb.peer.HyperGraphPeer;
import org.hypergraphdb.util.HGUtils;

import java.io.File;
import java.util.concurrent.Future;

/**
 * Hyper Graph
 * Peer to Peer setup init peer
 *
 * @author Fabrizio Parrillo
 */
public class P2P {

    static String config = "/home/ubuntu/peerConfig.json";

    public static void main(String[] args) throws InterruptedException {
        HyperGraphPeer peer = getPeer();
        while (true)
            try {
                Thread.sleep(5000);
                peer.getGraph().runMaintenance();
            } catch (InterruptedException ex) {
                break;
            }

    }

    private static HyperGraphPeer startPeer(File config) {
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

    public static HyperGraphPeer getPeer() throws InterruptedException {
        HyperGraphPeer peer;
        File config = new File(P2P.config);
        peer = P2P.startPeer(config);
        NodeQueries.addIndex(peer.getGraph());
        while (peer.getConnectedPeers().isEmpty()) Thread.sleep(500);

        System.out.println("Connected peers to " + peer.getConfiguration().at("interfaceConfig").at("user"));
        peer.getConnectedPeers().forEach(System.out::println);
        return peer;
    }
}
