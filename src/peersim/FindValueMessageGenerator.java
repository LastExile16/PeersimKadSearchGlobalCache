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

	// ______________________________________________________________________________________________
	/**
	 * MSPastry Protocol to act
	 */
	private final static String PAR_PROT = "protocol";

	/**
	 * MSPastry Protocol ID to act
	 */
	private final int pid;


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
			
			// TODO - random value should follow normal distribution
			// int rand = ThreadLocalRandom.current().nextInt(0, (int) StoreMessageGenerator.df.count().col(0).get(0));
			int rand = ThreadLocalRandom.current().nextInt(0, StoreMessageGenerator.generatedKeyList_factor.size());
			if(StoreMessageGenerator.generatedKeyList_factor != null && !StoreMessageGenerator.generatedKeyList_factor.isEmpty()) {
				BigInteger key = (BigInteger) new ArrayList<>(StoreMessageGenerator.generatedKeyList_factor.keySet()).get(rand);
				if(!StoreMessageGenerator.generatedKeyList_status.get(key)) {
					retry++;
					continue;
				}
				Message m = Message.makeFindValue(key);
				m.timestamp = CommonState.getTime();
				m.dest = key;
	
				return m;
			}
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
		Node start;
		do {
			start = Network.get(CommonState.r.nextInt(Network.size()));
		} while ((start == null) || (!start.isUp()));
		Message generatedQuery = generateFindValueMessage();
		// return without adding any event if message generation was unsuccessful.
		if (generatedQuery == null) { 
			System.out.println("No kv available in the network to generate query. No event added");
			return false; 
		}
		
		// send message
		if(StoreMessageGenerator.generatedKeyList_factor != null && !StoreMessageGenerator.generatedKeyList_factor.isEmpty()) {
			EDSimulator.add(0, generatedQuery, start, pid);
		}

		return false;
	}

	// ______________________________________________________________________________________________

} // End of class
// ______________________________________________________________________________________________
