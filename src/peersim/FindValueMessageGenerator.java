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
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;


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
	
	private static ArrayList<BigInteger> KeysWithFrequency = new ArrayList<>();
	private static Map<BigInteger, Boolean> halfhmap1 = new HashMap<>();
	private static Map<BigInteger, Boolean> halfhmap2 = new HashMap<>();
	private static ArrayList<BigInteger> halfFrequency1 = new ArrayList<>();
	private static ArrayList<BigInteger> halfFrequency2 = new ArrayList<>();
	private static boolean keyListContructionDone = false;
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
	private Message generateFindValueMessage(){
		
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
	}
	
	/**
	 * generating queries according to the frequency of the keyword in the original dataset
	 * @return
	 * 		the randomly chosen message
	 */
	private Message generateFindValueMessageTrueProbability(){
		ArrayList<BigInteger> tmp = null;
		if (!keyListContructionDone) {
			// generate a list of truly stored keys.
			/*for(BigInteger keyStatus: StoreMessageGenerator.generatedKeyList_status.keySet()) {
				if(StoreMessageGenerator.generatedKeyList_status.get(keyStatus)) {
					int frequency = StoreMessageGenerator.generatedKeyList_weight.getOrDefault(keyStatus, -1);
					for(int i=0; i<frequency; i++) {
						KeysWithFrequency.add(keyStatus);
					}
				}
			}*/
			keyListContructionDone = true;
			// System.out.print(keys.size());
			
			// locality
		    int count=0;
		    
		    // creating two halves of keyword set
		    for(Map.Entry<BigInteger, Boolean> entry : StoreMessageGenerator.generatedKeyList_status.entrySet()) {
		        (count<(StoreMessageGenerator.generatedKeyList_status.size()/3) ? halfhmap1:halfhmap2).put(entry.getKey(), entry.getValue());
		        count++;
		    }
		    
		    // adding frequency to each group
		    // group 1
		    for(BigInteger keyStatus: halfhmap1.keySet()) {
				if(halfhmap1.get(keyStatus)) {
					int frequency = StoreMessageGenerator.generatedKeyList_weight.getOrDefault(keyStatus, -1);
					for(int i=0; i<frequency; i++) {
						halfFrequency1.add(keyStatus);
					}
				}
			}
		    
		    // group 2
		    for(BigInteger keyStatus: halfhmap2.keySet()) {
				if(halfhmap2.get(keyStatus)) {
					int frequency = StoreMessageGenerator.generatedKeyList_weight.getOrDefault(keyStatus, -1);
					for(int i=0; i<frequency; i++) {
						halfFrequency2.add(keyStatus);
					}
				}
			}
		    
		}
		
		int randGroup = ThreadLocalRandom.current().nextInt(0, 10);
	    
	    if(randGroup < 9 ) {
	    	tmp = halfFrequency1;
	    }
	    else {
	    	tmp = halfFrequency2;
	    }
		// randomly choosing a message
		int rand = 0;
		try {
			rand = CommonState.r.nextInt(tmp.size());
		} catch(IllegalArgumentException ex) { // IllegalArgumentException happens when generatedKeyList_weight.size = 0
			ex.printStackTrace();
			return null;
		}
		
		BigInteger key = tmp.get(rand);
		if(issuedQuery.containsKey(key)) {
			//++KademliaObserver.h;
			issuedQuery.put(key, issuedQuery.get(key)+1);
		}else {
			issuedQuery.put(key, 0);
		}
		
		Message m = Message.makeFindValue(key);
		//System.out.print(m.body+" rand: " + rand+" - ");
		m.timestamp = CommonState.getTime();
		m.dest = key;
		
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
