package peersim;

import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Control;
import peersim.core.Network;
import peersim.core.Node;
import peersim.edsim.EDSimulator;
//import sun.nio.ch.Net; **The type sun.nio.ch.Net is not accessible

import java.math.BigInteger;

/**
 * This control generates random search traffic from nodes to random destination node.
 * 
 * @author Daniele Furlan, Maurizio Bonani
 * @version 1.0
 */

// ______________________________________________________________________________________________
public class TrafficGenerator implements Control {

	// ______________________________________________________________________________________________
	/**
	 * MSPastry Protocol to act
	 */
	private final static String PAR_PROT = "protocol";

	/**
	 * MSPastry Protocol ID to act
	 */
	private final int pid;

	private UniformRandomGenerator urg;

	// ______________________________________________________________________________________________
	public TrafficGenerator(String prefix) {
		pid = Configuration.getPid(prefix + "." + PAR_PROT);

	}

	// ______________________________________________________________________________________________
	/**
	 * generates a random find node message, by selecting randomly (random but active) the destination.
	 * 
	 * @return Message
	 */
	private Message generateFindNodeMessage() {
		Message m = Message.makeFindNode("Automatically Generated Traffic");
		m.timestamp = CommonState.getTime();

		// existing active destination node
		Node n = Network.get(CommonState.r.nextInt(Network.size()));
		while (!n.isUp()) {
			n = Network.get(CommonState.r.nextInt(Network.size()));
		}
		m.dest = ((KademliaProtocol) (n.getProtocol(pid))).nodeId;

		return m;
	}




	// ______________________________________________________________________________________________
	/**
	 * every call of this control generates and send a random find node message
	 * 
	 * @return boolean
	 */
	public boolean execute() {
		Node start;
		do {
			start = Network.get(CommonState.r.nextInt(Network.size()));
		} while ((start == null) || (!start.isUp()));

		// send message
		EDSimulator.add(0, generateFindNodeMessage(), start, pid);
		return false;
	}

	// ______________________________________________________________________________________________

} // End of class
// ______________________________________________________________________________________________
