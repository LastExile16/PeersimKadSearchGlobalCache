package peersim;

import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Control;
import peersim.core.Network;
import peersim.core.Node;
import peersim.edsim.EDSimulator;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import joinery.DataFrame;
import static java.lang.Math.toIntExact;
/**
 * This control generates random store-message (kv tuples) traffic from nodes to random destination node.
 * 
 * @author chinese guy github - zysung
 * @author Nawras Nazar
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
	 * not removed so as not to break the prev version of the project
	 */
	public static List<String> generateStoreVals= new ArrayList<>();
	/**
	 * keep the list of the generated keys with their status, true: stored successfully, false: store failed
	 * this variable should replace generateStoreVals  
	 */
	public static HashMap<BigInteger, Boolean> generatedKeyList_status = new HashMap<BigInteger, Boolean>();
	
	/**
	 * keep the list of the generated keys a long with the number of times the query issued in the dataset
	 */
	public static HashMap<BigInteger, Integer> generatedKeyList_factor = new HashMap<BigInteger, Integer>();
	
	public final static DataFrame df;
	static {
		DataFrame tmp = null;
		try {
			tmp = DataFrame.readCsv("frequency-search-logs-with-header.csv");
		} catch (IOException e) {
			System.err.println("Error reading the data-set");
			e.printStackTrace();
		}
		df = tmp;
	}
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
	/**
	 * Generate Message by selecting kv randomly from the provided data-set, hash the key with SHA1 then store kv to a {@link StoreFile } object
	 * the method retries key picking for 20 times before returning null, if it happened to be already in {@link #generatedKeyList}
	 * @return
	 * 		returns the generated message of type MSG_STORE_REQUEST
	 */
	private Message generateStoreMessageFromDataset() {
		
		Message m = null;
		int retry = 0;
		while(retry<20) {
			// nextInt is normally exclusive of the top value [min, max)
			int rand = ThreadLocalRandom.current().nextInt(0, (int) df.count().col(0).get(0));
			String key = (String) df.col(0).get(rand);
			Set<String> value = new HashSet<String>(Arrays.asList(((String) df.col(1).get(rand)).split(", ")));
			BigInteger hashed_key = null;
			try {
				hashed_key = new BigInteger(SHA1.shaEncode(key), 16);
			} catch (Exception e) {
				System.err.println("Hashing the key failed");
				e.printStackTrace();
			}
			// if the message was already generated try a new message otherwise return null
			// TODO - the generated key should be skipped if it's status was true, bcz false means its not stored yet
			if(generatedKeyList_status.containsKey(hashed_key)) {
				retry++;
				continue;
			}
			// msg size = no.of elements*4 (each element is 4 bytes) 
			StoreFile sf = new StoreFile(hashed_key, value, value.size()*4);
			generatedKeyList_status.put(hashed_key, false); // so that FindValGenerator will generate random available find operations
			generatedKeyList_factor.put(hashed_key, toIntExact((Long)df.col(2).get(rand)) );
			
			m = Message.makeStoreReq(sf);
			m.timestamp = CommonState.getTime();
			m.dest = hashed_key;
			System.out.println("Debugging msgGenerator - trial:"+ retry +" : "+sf);
			break;
		}
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
		Message generatedMessage = generateStoreMessageFromDataset();
		// return without adding any event if message generation was unsuccessful.
		if (generatedMessage == null) { 
			System.out.println("Unique message generation failed. No event added");
			return false; 
		}
		EDSimulator.add(0, generatedMessage, randomNode, pid);

		return false;
	}

	// ______________________________________________________________________________________________

} // End of class
// ______________________________________________________________________________________________
