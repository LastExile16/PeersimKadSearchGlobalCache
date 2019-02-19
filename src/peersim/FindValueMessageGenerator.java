package peersim;

import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Control;
import peersim.core.Network;
import peersim.core.Node;
import peersim.edsim.EDSimulator;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import joinery.DataFrame;

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
	
	private static ArrayList<BigInteger> RepeatedKeys = new ArrayList<>();
	private static boolean keyListContructionDone = false;

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
	
	
	private Message generateFindValueMessageTrueProbability(){
		
		if (!keyListContructionDone) {
			// generate a list of truly stored keys.
			for(BigInteger keyStatus: StoreMessageGenerator.generatedKeyList_status.keySet()) {
				if(StoreMessageGenerator.generatedKeyList_status.get(keyStatus)) {
					int frequency = StoreMessageGenerator.generatedKeyList_weight.getOrDefault(keyStatus, -1);
					for(int i=0; i<frequency; i++) {
						RepeatedKeys.add(keyStatus);
					}
				}
			}
			keyListContructionDone = true;
			// System.out.print(keys.size());
		}
		
		int rand = 0;
		try {
			rand = CommonState.r.nextInt(RepeatedKeys.size());
		} catch(IllegalArgumentException ex) { // IllegalArgumentException happens when generatedKeyList_weight.size = 0
			ex.printStackTrace();
			return null;
		}
		BigInteger key = RepeatedKeys.get(rand);
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
			rand = ThreadLocalRandom.current().nextInt(0, Network.size());
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
