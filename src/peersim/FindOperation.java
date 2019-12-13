package peersim;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.HashMap;
import java.util.TreeMap;

/**
 * This class represents a find operation and offer the methods needed to maintain and update the closest set.<br>
 * It also maintains the number of parallel requsts that can has a maximum of ALPHA.
 * 
 * @author Daniele Furlan, Maurizio Bonani, Nawras Nazar
 * @version 1.0
 * 
 */
public class FindOperation {

	/**
	 * unique sequence number generator of the operation
	 */
	private static long OPERATION_ID_GENERATOR = 0;

	/**
	 * represent univocally the find operation
	 */
	public long operationId;

	/**
	 * parent contains the hash of the whole (original) issued query before dividing it into sub keywords. <br>
	 * used in multi keyword query to recognize the parts of the same query and merge the result.
	 * value = null: the search is single word
	 * value != null: search is multi keyword 
	 */
	public BigInteger parent;
	
	/**
	 * keywords contains the original sub keywords. this helps us to find the relating keywords to a #parent variable.
	 */
	public BigInteger[] keywords;
	/**
	 * Id of the node to find
	 */
	public BigInteger destNode;

	/**
	 * Body of the original find message
	 */
	public Object body;

	/**
	 * number of available find request message to send (it must be always less than ALPHA)
	 */
	public int available_requests;
	
	/**
	 * number of outstanding messages sent to get the result of a query from the nodes in the {@link #closestSet}
	 * increased when message sent {@link peersim.Message#MSG_FINDVALUE} and decreased when receiving the answer {@link peersim.Message#MSG_RETURNVALUE} or {@link peersim.Message#MSG_RETURNVALUE_FROM_CACHE}
	 *  
	 */
	public int outstanding_find_requests;
	/**
	 * Start timestamp of the search operation
	 */
	protected long timestamp = 0;

	/**
	 * Number of hops the message did
	 */
	protected int nrHops = 0;

	/**
	 * This map contains the K closest nodes and corresponding boolean value that indicates if the nodes has been already queried
	 * or not
	 */
	protected HashMap<BigInteger, Boolean> closestSet;

	//test
	protected TreeMap<BigInteger,Integer> nodeSpace = new TreeMap<>();
	
	 
	/**
	 * default constructor
	 * 
	 * @param destNode
	 *            Id of the node to find
	 */
	public FindOperation(BigInteger destNode, long timestamp) {
		this.destNode = destNode;
		this.timestamp = timestamp;

		// set a new find id
		operationId = OPERATION_ID_GENERATOR++;

		// set availabe request to ALPHA
		available_requests = KademliaCommonConfig.ALPHA;
		
		// set outstanding messages to 0 (no outstanding messages yet)
		outstanding_find_requests = 0;
		
		// initialize closestSet
		closestSet = new HashMap<BigInteger, Boolean>();
		
		// query operations are single keyword by default. no parent
		parent = null;
	}

	/**
	 * update closestSet with the new information received
	 * 
	 * @param neighbours 	用节点的k个neighbours更新节点该次fop的closeSet
	 */
	public void elaborateResponse(BigInteger[] neighbours) {
		// update responseNumber
		available_requests++;

		// add to closestSet
		for (BigInteger n : neighbours) {

			if (n != null) {
				if (!closestSet.containsKey(n)) {
					if (closestSet.size() < KademliaCommonConfig.K) { // add directly
						closestSet.put(n, false);
					} else { // find in the closest set if there are nodes with less distance
						BigInteger newdist = Util.distance(n, destNode);

						// find the node with max distance
						BigInteger maxdist = newdist;
						BigInteger nodemaxdist = n;
						for (BigInteger i : closestSet.keySet()) {
							BigInteger dist = Util.distance(i, destNode);

							if (dist.compareTo(maxdist) > 0) {
								maxdist = dist;
								nodemaxdist = i;
							}
						}
						//将最长距离节点移除，并将新的neighbor加入closeSet
						//Remove the longest distance node and add the new neighbor to the closeSet
						if (nodemaxdist.compareTo(n) != 0) {
							closestSet.remove(nodemaxdist);
							closestSet.put(n, false);
							System.out.println("new neighbor added");
						}
					}
				}
			}
		}

		/*String s = "closestSet to " + destNode + "\n";
		for (BigInteger clos : closestSet.keySet()) {
			 s+= clos + "-";
		}
		System.out.println(s);*/

	}

	/**
	 * get the first neighbor in closest set which has not been already queried
	 * 拿closeSet里面最近那个节点
	 * @return the Id of the node or null if there aren't available node
	 */
	public BigInteger getNeighbour() {
		// find closest neighbour ( the first not already queried)
		BigInteger res = null;
		for (BigInteger n : closestSet.keySet()) {
			if (n != null && closestSet.get(n) == false) {
				if (res == null) {
					res = n;
				} else if (Util.distance(n, destNode).compareTo(Util.distance(res, destNode)) < 0) {
					res = n;
				}
			}
		}

		// Has been found a valid neighbour
		if (res != null) {
			closestSet.remove(res);
			closestSet.put(res, true);
			available_requests--; // decrease available request
		}

		return res;
	}

	@Override
	public String toString() {
		return "FindOperation \n[operationId=" + operationId + ",\n" +
				"parent=" + parent + ",\n" +
				"keywords=" + Arrays.toString(keywords) + ",\n" +
				"destNode=" + destNode + ",\n" +
				"body=" + body + ",\n" +
				"available_requests=" + available_requests + ",\n" +
				"timestamp=" + timestamp + ",\n" +
				"nrHops=" + nrHops + ",\n" +
				"closestSet=" + closestSet + ",\n" +
				"nodeSpace=" + nodeSpace + "]";
	}
	
}
