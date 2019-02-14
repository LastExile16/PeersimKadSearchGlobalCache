package peersim;

import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Control;
import peersim.core.Network;
import peersim.core.Node;
import peersim.edsim.EDSimulator;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

/**
 * This control generates random store-message (kv tuples) traffic from nodes to random destination node.
 * 
 * @author chinese guy github - zysung
 * @author Nawras
 */

// ______________________________________________________________________________________________
public class StoreMessageGenerator implements Control {

	// ______________________________________________________________________________________________
	/**
	 * MSPastry Protocol to act
	 */
	private final static String PAR_PROT = "protocol";

	/**
	 * MSPastry Protocol ID to act
	 */
	private final int pid;
	
	/**
	 * generated message value list.
	 * So that FindValGenerator will generate random available find operations
	 */
	public static List<String> generateStoreVals= new ArrayList<>();
	/**
	 * keep the list of the generated keys with their status, true: stored successfully, false: store failed
	 * this variable should replace generateStoreVals
	 */
	public static HashMap<BigInteger, Boolean> generatedKeyList= new HashMap<BigInteger, Boolean>();

	// ______________________________________________________________________________________________
	public StoreMessageGenerator(String prefix) {
		pid = Configuration.getPid(prefix + "." + PAR_PROT);
	}

	/**
	 * Generate a simulated StoreFile with a random value from UUID and the 160-bit key of SHA1(UUID) that will be changed to 256 bit SHA256
	 * @return
	 * 		returns the generated message of type MSG_STORE_REQUEST
	 */
	//______________________________________________________________________________________________
	private Message generateStoreMessage(){
		String value = UUID.randomUUID().toString().replace("-","");
		BigInteger key = null;
		try {
			key = new BigInteger(SHA1.shaEncode(value), 16);
		} catch (Exception e) {
			e.printStackTrace();
		}
		StoreFile sf = new StoreFile(key, value);
		generateStoreVals.add(value); // so that FindValGenerator will generate random available find operations
		Message m = Message.makeStoreReq(sf);
		m.timestamp = CommonState.getTime();
		m.dest = key;
		System.out.println("Debugging msgGenerator: "+sf);
		return m;
	}



	// ______________________________________________________________________________________________
	/**
	 * every call of this control generates and send a random find node message
	 * 
	 * @return boolean
	 */
	public boolean execute() {
		Node randomNode;
		do {
			randomNode = Network.get(CommonState.r.nextInt(Network.size()));
		} while ((randomNode == null) || (!randomNode.isUp()));

		// send message
		System.out.println("randomly chosen node: "+((KademliaProtocol)randomNode.getProtocol(pid)).getNodeId());
		EDSimulator.add(0, generateStoreMessage(), randomNode, pid);

		return false;
	}

	// ______________________________________________________________________________________________

} // End of class
// ______________________________________________________________________________________________
