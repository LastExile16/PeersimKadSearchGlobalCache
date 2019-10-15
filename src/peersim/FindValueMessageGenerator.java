package peersim;

import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Control;
import peersim.core.Network;
import peersim.core.Node;
import peersim.edsim.EDSimulator;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;



/**
 * This control generates random search traffic from nodes to random destination node.
 * 
 * @author Daniele Furlan, Maurizio Bonani
 * @version 1.0
 * @author Nawras Nazar
 */

// ______________________________________________________________________________________________
public class FindValueMessageGenerator implements Control {
public static int i=0;
	// ______________________________________________________________________________________________
	/**
	 * MSPastry Protocol to act
	 */
	private final static String PAR_PROT = "protocol";

	/**
	 * MSPastry Protocol ID to act
	 */
	private final int pid;
	public static Map<BigInteger, Integer> issuedQuery = new HashMap<>();
	// ______________________________________________________________________________________________
	public FindValueMessageGenerator(String prefix) {
		pid = Configuration.getPid(prefix + "." + PAR_PROT);
	}

    /**
     * Generate a simulated StoreFile with a random key of 160 bits and a value of an empty obj
	 * @return
     */
	//______________________________________________________________________________________________
	/*private Message generateFindValueMessage(){
		
		int retry = 0;
		while(retry<10) {
			
			// int rand = ThreadLocalRandom.current().nextInt(0, (int) StoreMessageGenerator.df.count().col(0).get(0));
			int rand = 0;
			try {
				// rand = ThreadLocalRandom.current().nextInt(0, StoreMessageGenerator.generatedKeyList_weight.size());
				
				rand = CommonState.r.nextInt(StoreMessageGenerator.generatedKeyList_weight.size());
			} catch(IllegalArgumentException ex) { // IllegalArgumentException happens when generatedKeyList_weight.size = 0
				ex.printStackTrace();
				return null;
			}
			
			if(StoreMessageGenerator.generatedKeyList_weight != null && !StoreMessageGenerator.generatedKeyList_weight.isEmpty()) {
				BigInteger key = (BigInteger) new ArrayList<>(StoreMessageGenerator.generatedKeyList_weight.keySet()).get(rand);
				if(!StoreMessageGenerator.generatedKeyList_status.get(key)) {
					retry++;
					continue;
				}
				// key = new BigInteger("679695804144180158154957817433688509811568326000");
				Message m = Message.makeFindValue(key);
				//System.out.print(m.body+" rand: " + rand+" - ");
				m.timestamp = CommonState.getTime();
				m.dest = key;
				//System.out.println(++i);
				return m;
			}
		}
		return null;
	}*/
	
	/**
	 * generating queries according to the frequency of the keyword in the original dataset that is according to the weight of each keyword
	 * @return  Message
	 * 		The randomly chosen message
	 */
	private Message generateFindValueMessageTrueProbability(){
		RandomCollection<BigInteger> rc = new RandomCollection<>();
		for(Map.Entry<BigInteger, Integer> entry : StoreMessageGenerator.generatedKeyList_weight.entrySet()) {
			// if the key is stored successfully in some nodes then add it to the RandomCollection rc
			if(StoreMessageGenerator.generatedKeyList_status.get(entry.getKey())) {
				rc.add(entry.getValue(), entry.getKey());
			}
		}
		
		BigInteger key = null;
		BigInteger multi_key_q_hash = null;
		String multi_key_q_str = "";
		// make sure to get more than one random key:
		// int r_no_of_keys = CommonState.r.nextInt(2)+2; // 2 or 3
		int r_no_of_keys = CommonState.r.nextInt(3)+1; // 1 or 2 or 3
		// int r_no_of_keys = CommonState.r.nextInt(1)+1;
		// System.err.println(r_no_of_keys);
		List<BigInteger> multi_keys_q_arr = new ArrayList<BigInteger>();
		while(r_no_of_keys>0) {
			// get a random key
			key = rc.next();
			//don't select the same key again in the same query
			/*
			 * If I want to consider duplicate keys in the same query, then I have to confirm the following:
			 * 1- searchResult uses key to find out if search is finished or not, so when we have 2 keys in a query
			 * 		and both of them are the same, then we don't need to send two FOP s (unless we practically check the
			 * 		content of the dataset)
			 * 2- findValue() should also consider this situation since we have checking to find out the element. 
			 */
			if(multi_keys_q_arr.contains(key)) {
				continue;
			}
			multi_keys_q_arr.add(key);
			//multi_key_q_str += key.toString();
			r_no_of_keys--;
		}
		// Sorting: since the values have same length, I'll get the correct sort even they are stored as strings not integers
		Collections.sort(multi_keys_q_arr);
		for(BigInteger k : multi_keys_q_arr) {
			multi_key_q_str += k.toString();
		}
		
		try {
			// hash the new combined query
			multi_key_q_hash = new BigInteger(SHA1.shaEncode(multi_key_q_str), 16);
			// System.out.println(multi_key_q_hash);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		Message m = Message.makeFindValue((BigInteger[])multi_keys_q_arr.toArray(new BigInteger[multi_keys_q_arr.size()]));
		//System.out.print(m.body+" rand: " + rand+" - ");
		m.timestamp = CommonState.getTime();
		
		/*
		 * originally the m.dest is used to find out the closest node to the given value, 
		 * but since I've added multikeyword query, first I generate an array of keys and store it in m.body
		 * then store the combined hash in the m.dest.
		 * after that in the KademliaProtocol.find() I create different messages for each of the values in the m.body then make the
		 * new messages' destination to be that value. So whatever I give here to the m.dest it won't affect the followup processes
		 */
		m.dest = multi_key_q_hash;
		// System.out.println(key);
		// System.exit(1);
		//m.body = key;
		//m.dest = key;
		// does the query issued before.
		if(KademliaObserver.overallIssuedQueries.containsKey(multi_key_q_hash)) {
			KademliaObserver.overallIssuedQueries.put(multi_key_q_hash, KademliaObserver.overallIssuedQueries.get(multi_key_q_hash)+1);
			KademliaObserver.duplicateQuery.add(1);
		}else {
			KademliaObserver.overallIssuedQueries.put(multi_key_q_hash, 0);
		}
		
		return m;
	}
	

	//______________________________________________________________________________________________
	/**
	 * every call of this control generates and send a random find node message
	 * 
	 * @return boolean
	 */
	public boolean execute() {
		Node start;
		do {
			int rand = CommonState.r.nextInt(Network.size());
			// rand = ThreadLocalRandom.current().nextInt(0, Network.size());
			// System.out.println("the node ind " + rand);
			start = Network.get(rand);
			// the node 162 always failed to find msg 679695804144180158154957817433688509811568326000
			// start = Network.get(162);
		} while ((start == null) || (!start.isUp()));
		
		/*Message generatedQuery = generateFindValueMessage();
		// return without adding any event if message generation was unsuccessful.
		
		 if (generatedQuery == null) { 
		 System.out.println("No kv available in the network to generate query. No event added");
			return false; 
		}*/
		
		Message generatedQuery = generateFindValueMessageTrueProbability();
		if(generatedQuery == null) {
			return false;
		}
		
		// send message
		EDSimulator.add(0, generatedQuery, start, pid);
		
		return false;
	}

	// ______________________________________________________________________________________________

} // End of class
// ______________________________________________________________________________________________
