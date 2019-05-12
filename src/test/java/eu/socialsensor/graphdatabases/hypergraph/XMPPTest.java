package eu.socialsensor.graphdatabases.hypergraph;

import org.hypergraphdb.peer.HyperGraphPeer;

import java.io.File;

import static com.google.common.io.Resources.getResource;

public class XMPPTest
{
	public static void main(String [] argv)
	{
		File config = new File(getResource("hgp2pA.json").getPath());
		HyperGraphPeer peer = new HyperGraphPeer(config);
//		XMPPPeerInterface xmpp = new XMPPPeerInterface();
//		xmpp.configure((Map)config.get("interfaceConfig"));
//		xmpp.start();
//		xmpp.setMessageHandler(new MessageHandler()
//		{
//			public void handleMessage(Message msg)
//			{
//				System.out.println("Got message: " + msg);
//			}
//		}
//		);
		peer.start(null, null);
		while (true)
		try { Thread.sleep(2000); }
		catch (Throwable t) { }
	}
}
