package peersim;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.TreeMap;

/**
 * Gives an implementation for the rounting table component of a kademlia node
 * 
 * @author Daniele Furlan, Maurizio Bonani
 * @version 1.0
 */
public class RoutingTable implements Cloneable {

	// node ID of the node
	public BigInteger nodeId = null;

	// k-buckets
	public TreeMap<Integer, KBucket> k_buckets = null;

	// ______________________________________________________________________________________________
	/**
	 * instanciates a new empty routing table with the specified size
	 */
	public RoutingTable() {
		k_buckets = new TreeMap<Integer, KBucket>();
		// initialize k-bukets
		for (int i = 0; i <= KademliaCommonConfig.BITS; i++) {
			k_buckets.put(i, new KBucket());
		}
	}

	/**
	 *  add a neighbour to the correct k-bucket
	 * @param node
	 */
	public void addNeighbour(BigInteger node) {
		// get the length of the longest common prefix (correspond to the correct k-bucket)
		//找到某节点（参数）与本节点之间的最长前缀长度，也就是k-bucket的编号
		int prefix_len = Util.prefixLen(nodeId, node);

		// add the node to the k-bucket 将节点加入对应的k-bucket
		k_buckets.get(prefix_len).addNeighbour(node);
	}

	/**
	 * remove a neighbour from the correct k-bucket
	 * @param node
	 */
	public void removeNeighbour(BigInteger node) {
		// get the lenght of the longest common prefix (correspond to the correct k-bucket)
		int prefix_len = Util.prefixLen(nodeId, node);

		// add the node to the k-bucket
		k_buckets.get(prefix_len).removeNeighbour(node);
	}

	/**
	 *  return the closest neighbour to a key from the correct k-bucket
	 * @param key
	 * 			K-nodes must be close to 'key'
	 * @param src
	 * 			the invoker nodeId, so that we can remove ourself from the K closest nodes
	 * @return
	 * 			List of K closest nodeIds from the correct k-bucket
	 */
	public BigInteger[] getNeighbours(final BigInteger key, final BigInteger src) {
		// resulting neighbours
		BigInteger[] result = new BigInteger[KademliaCommonConfig.K];

		// neighbour candidates
		ArrayList<BigInteger> neighbour_candidates = new ArrayList<BigInteger>();

		// get the length of the longest common prefix
		//Calculate the common prefix length of the target node and the current node to determine which bucket is located in the kbuckets
		int prefix_len = Util.prefixLen(nodeId, key);//计算目标节点与当前节点的共同前缀长确定kbuckets中位于哪个桶中

		// return the k-bucket if is full
		if (k_buckets.get(prefix_len).neighbours.size() >= KademliaCommonConfig.K) { // If the size of the bucket in kbuckets is full
			return k_buckets.get(prefix_len).neighbours.keySet().toArray(result);// Return the entire kbucket to src
		}

		// else get k closest node from all k-buckets
		prefix_len = 0;
		while (prefix_len < KademliaCommonConfig.ALPHA) {  // Add some nodeID to the candidate list
			neighbour_candidates.addAll(k_buckets.get(prefix_len).neighbours.keySet());
			// remove source id
			neighbour_candidates.remove(src);
			prefix_len++;
		}

		// create a map (distance, node)
		TreeMap<BigInteger, BigInteger> distance_map = new TreeMap<BigInteger, BigInteger>();

		for (BigInteger node : neighbour_candidates) {// Sort candidate list nodes based on distance
			distance_map.put(Util.distance(node, key), node);
		}

		int i = 0;
		for (BigInteger iii : distance_map.keySet()) {// Select the nearest k nodes to return
			if (i < KademliaCommonConfig.K) {
				result[i] = distance_map.get(iii);
				i++;
			}
		}

		return result;
	}

	////// special and temporary method
	/**
	 *  return the closest neighbour to a key from the correct k-bucket, only used in the dataset distribution
	 *  the part to return all of the bucket at once is remove so that the code will check the limited closest nodes
	 * @param key
	 * 			K-nodes must be close to 'key'
	 * @param src
	 * 			the invoker nodeId, so that we can remove ourself from the K closest nodes
	 * @return
	 * 			List of K closest nodeIds from the correct k-bucket
	 */
	public BigInteger[] getNeighbours2(final BigInteger key, final BigInteger src) {
		// resulting neighbours
		BigInteger[] result = new BigInteger[KademliaCommonConfig.K];

		// neighbour candidates
		ArrayList<BigInteger> neighbour_candidates = new ArrayList<BigInteger>();

		// get the length of the longest common prefix
		//Calculate the common prefix length of the target node and the current node to determine which bucket is located in the kbuckets
		int prefix_len = Util.prefixLen(nodeId, key);//计算目标节点与当前节点的共同前缀长确定kbuckets中位于哪个桶中
		// else get k closest node from all k-buckets
		prefix_len = 0;
		while (prefix_len < KademliaCommonConfig.BITS) {  // Add some nodeID to the candidate list
			neighbour_candidates.addAll(k_buckets.get(prefix_len).neighbours.keySet());
			// remove source id
			neighbour_candidates.remove(src);
			prefix_len++;
		}

		// create a map (distance, node)
		TreeMap<BigInteger, BigInteger> distance_map = new TreeMap<BigInteger, BigInteger>();

		for (BigInteger node : neighbour_candidates) {// Sort candidate list nodes based on distance
			distance_map.put(Util.distance(node, key), node);
		}

		int i = 0;
		for (BigInteger iii : distance_map.keySet()) {// Select the nearest k nodes to return
			if (i < KademliaCommonConfig.K) {
				result[i] = distance_map.get(iii);
				i++;
			}
		}

		return result;
	}
///////////
	// ______________________________________________________________________________________________
	public Object clone() {
		RoutingTable dolly = new RoutingTable();
		for (int i = 0; i < KademliaCommonConfig.BITS; i++) {
			k_buckets.put(i, new KBucket());// (KBucket) k_buckets.get(i).clone());
		}
		return dolly;
	}

	// ______________________________________________________________________________________________
	/**
	 * print a string representation of the table
	 * 
	 * @return String
	 */
	public String toString() {
		return "";
	}
	// ______________________________________________________________________________________________

} // End of class
// ______________________________________________________________________________________________
