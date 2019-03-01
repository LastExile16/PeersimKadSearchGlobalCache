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
	public static HashMap<BigInteger, Integer> generatedKeyList_weight = new HashMap<BigInteger, Integer>();
	
	/**
	 * number of overloaded nodes (i.e. remaining storage = 0)
	 */
	public static Set<BigInteger> overLoadedNodes = new HashSet<>();
	
	public final static DataFrame<?> df;
	static {
		DataFrame<?> tmp = null;
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
			generatedKeyList_weight.put(hashed_key, toIntExact((Long)df.col(2).get(rand)) );
			
			m = Message.makeStoreReq(sf);
			m.timestamp = CommonState.getTime();
			m.dest = hashed_key;
			System.out.println("Debugging msgGenerator - trial:"+ retry +" : "+sf);
			break;
		}
		return m;
	}
	/**
	 * distribution needed to be done only one time. 
	 */
	public boolean distributionFinished = false;
	/**
	 * distribute dataset messages over the network
	 * each time the message distribution starts from a random node
	 */
	public void distributeMessages() {
		if (distributionFinished) return;
		int networkSize = Network.size();
		int datasetSize = df.col(0).size();
		BigInteger newNodeId = new BigInteger(KademliaCommonConfig.BITS, CommonState.r);
		
		/*Node randomNode = Network.get(1);
		BigInteger newNodeId = ((KademliaProtocol)randomNode.getProtocol(pid)).getNodeId();*/
		// XXX - the stdout is only for debugging
		// System.out.println("new dummy node id: "+ newNodeId);
		
		KademliaProtocol kd = new KademliaProtocol(newNodeId);
		kd.routingTable.nodeId = newNodeId;
		BigInteger[] nodeIdList = new BigInteger[networkSize];
		
		for (int i=0; i< networkSize; i++) {
			Node nd = Network.get(i);
			nodeIdList[i] = ((KademliaProtocol)nd.getProtocol(pid)).getNodeId();
			//System.out.println(nodeIdList[i]);
		}
		// XXX - the stdout is only for debugging
		// System.out.println("node list size: "+nodeIdList.length);
		
		
		int originalK = KademliaCommonConfig.K;
		// one bucket need to store half of the network ids, but remember, maybe in a network of 16 nodes, only one node get id in the first half and all others get id in the second half! which means this lonely node should have space to store ALL of the other nodes in the second half.
		KademliaCommonConfig.K = networkSize;
		for(int n=0; n<nodeIdList.length; n++) {
			kd.routingTable.addNeighbour(nodeIdList[n]);
		}
		
		KademliaCommonConfig.K = originalK;
		
		//for(int i =0; i< kd.routingTable.k_buckets.size(); i++) {
			//System.out.println(kd.routingTable.k_buckets);
		//}
		
		// TODO - don't declare inside the loop
		// String key = null;
		// Set<String> value = null;
		for (int i=0; i< datasetSize; i++) {
			
			// Node nd = Network.get(i % networkSize);
			// KademliaProtocol kd = (KademliaProtocol)nd.getProtocol(pid);
			
			String key = (String) df.col(0).get(i);
			BigInteger hashed_key = null;
			try {
				hashed_key = new BigInteger(SHA1.shaEncode(key), 16);
			} catch (Exception e) {
				System.err.println("Hashing the key failed");
				e.printStackTrace();
			}
			
			Set<String> value = new HashSet<String>(Arrays.asList(((String) df.col(1).get(i)).split(", ")));
			int weight = toIntExact((Long)df.col(2).get(i));
			
			BigInteger[] kClosestNodeIds = kd.routingTable.getNeighbours2(hashed_key, kd.getNodeId());
			// kk value is the key that never returns by some or all nodes
			// Object kk = "679695804144180158154957817433688509811568326000";

			// XXX - the stdout is only for debugging
			// System.out.println(i+"- data hashed key: " + hashed_key);
			// System.out.println("no. of closest nodes: " + kClosestNodeIds.length);
			/*for (BigInteger closeNode : kClosestNodes) {
				System.out.println(closeNode);
			}*/
			
			// msg size = no.of elements*4 (each element is 4 bytes) 
			StoreFile sf = new StoreFile(hashed_key, value, value.size()*4);
			
			boolean storeSucceed = false;
			// loop through the close nodes list and store the kv into them
			for (BigInteger closeNodeId : kClosestNodeIds) {
				Node tmp = nodeIdtoNode(closeNodeId, pid);
				if(!tmp.isUp()) {
					continue;
				}
				KademliaProtocol closeNodeKad = (KademliaProtocol) tmp.getProtocol(pid);
				if (closeNodeKad.getStoreCapacity() >= sf.getSize()) {
					
					// XXX - the stdout is only for debugging
					/*
					 * boolean cmp = closeNodeKad.getStoreCapacity() >= sf.getSize();
					 * System.out.print("closeNodeKad.getStoreCapacity() >= sf.getSize() " + cmp);
					 * System.out.print(" msg size: " + sf.getSize());
					 * System.out.print(" capacity: " + closeNodeKad.getStoreCapacity() + " = ");
					 * System.out.println( closeNodeKad.getStoreCapacity() - sf.getSize());
					 */
					
					closeNodeKad.setStoreMap(sf.getKey(), sf.getValue());
					closeNodeKad.setStoreCapacity(closeNodeKad.getStoreCapacity() - sf.getSize());
					KademliaObserver.real_store_operation.add(1);
					storeSucceed = true;
					// XXX - the stdout is only for debugging
					// System.out.println("store operation: " + closeNodeKad);
				}else {
					// XXX - the stdout is only for debugging
					//System.out.println("Node:" + closeNodeKad.getNodeId() + ":" + closeNodeKad.getStoreCapacity()
						//	+ " space storage is not enough to store kv data:" + sf.toString());
					overLoadedNodes.add(closeNodeId);
					KademliaObserver.real_store_fail_operation.add(1);
				}
			}
			
			generatedKeyList_status.put(hashed_key, storeSucceed); // so that FindValGenerator will generate random available find operations
			generatedKeyList_weight.put(hashed_key, weight );
			//break;
			
		}
		//System.out.println(generatedKeyList_status);
		KademliaObserver.overloadNode.add(overLoadedNodes.size());
		distributionFinished = true;
	}
	/**
	 * Originally from {@link KademliaProtocol} class
	 * Search through the network the Node having a specific node Id, by performing
	 * binary search (we concern about the ordering of the network). Finding a node
	 * by binary search
	 * 
	 * @param searchNodeId BigInteger
	 * @return Node
	 */
	private Node nodeIdtoNode(BigInteger searchNodeId, int kademliaid) {
		if (searchNodeId == null)
			return null;

		int inf = 0;
		int sup = Network.size() - 1;
		int m;

		while (inf <= sup) {
			m = (inf + sup) / 2;

			BigInteger mId = ((KademliaProtocol) Network.get(m).getProtocol(kademliaid)).nodeId;

			if (mId.equals(searchNodeId))
				return Network.get(m);

			if (mId.compareTo(searchNodeId) < 0)
				inf = m + 1;
			else
				sup = m - 1;
		}

		// perform a traditional search for more reliability (maybe the network is not
		// ordered)
		BigInteger mId;
		for (int i = Network.size() - 1; i >= 0; i--) {
			mId = ((KademliaProtocol) Network.get(i).getProtocol(kademliaid)).nodeId;
			if (mId.equals(searchNodeId))
				return Network.get(i);
		}

		return null;
	}
	// ______________________________________________________________________________________________
	/**
	 * every call of this control generates and send a random find node message
	 * 
	 * @return boolean
	 */
	public boolean execute() {
		
		/*
		 
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
		*/
		
		distributeMessages();
		/*int networkSize = Network.size();
		for (int i=0; i< networkSize; i++) {
			Node nd = Network.get(i);
			System.out.println(((KademliaProtocol)nd.getProtocol(pid)).getNodeId());
		}*/
		return false;
		// return true;
	}

	// ______________________________________________________________________________________________

} // End of class
// ______________________________________________________________________________________________
