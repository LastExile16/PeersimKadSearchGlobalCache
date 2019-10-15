package peersim;

/**
 * A Kademlia implementation for PeerSim extending the EDProtocol class.<br>
 * See the Kademlia bibliografy for more information about the protocol.
 *
 * 
 * @author Daniele Furlan, Maurizio Bonani
 * @version 1.0
 */

import java.math.BigInteger;
import java.util.*;
import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Network;
import peersim.core.Node;
import peersim.edsim.EDProtocol;
import peersim.edsim.EDSimulator;
import peersim.transport.UnreliableTransport;
//import sun.security.util.PendingException;

//__________________________________________________________________________________________________
public class KademliaProtocol implements Cloneable, EDProtocol {
	private int cacheCapacity = 1000;
	// VARIABLE PARAMETERS
	final String PAR_K = "K";
	final String PAR_ALPHA = "ALPHA";
	final String PAR_BITS = "BITS";
	final String PAR_CACHE = "CACHE";

	private static final String PAR_TRANSPORT = "transport";
	private static String prefix = null;
	private UnreliableTransport transport;
	private int tid;
	private int kademliaid;
	
	/**
	 * allow to call the service initializer only once
	 */
	private static boolean _ALREADY_INSTALLED = false;

	/**
	 * nodeId of this pastry node
	 */
	public BigInteger nodeId;

	/**
	 * routing table of this pastry node
	 */
	public RoutingTable routingTable;

	/**
	 * trace message sent for timeout purpose
	 */
	private TreeMap<Long, Long> sentMsg;

	/**
	 * find operations set
	 */
	private LinkedHashMap<Long, FindOperation> allIssuedfindOps;

	/**
	 * Node storage , <item key, providers>
	 */
	private TreeMap<BigInteger, Object> storeMap;

	/**
	 * send sth to store map <value, store times> how many times this msg has been
	 * stored. it counts number of successful MSG_STORE_RESP that are received from
	 * other nodes
	 */
	private Map<BigInteger, Integer> storeTimesMap;

	private boolean storeSucceedFlag = false;

	/**
	 * node store capacity
	 */
	private int storeCapacity;
	
	/**
	 * node cache capacity
	 */
	
	private LRUCache cache;
	/**
	 * Store the storage capacity sent by the node, and then send STORE after
	 * sorting.
	 */
	private Map<BigInteger, Integer> nodeSpace;

	private Set<String> receivedVals;

	private List<BigInteger> findVals;
	
	/**
	 * stores the issued queries (and their results when received)
	 * replaces {@link KademliaProtocol#findVals } and {@link KademliaProtocol#receivedVals} <br>
	 * 
	 * note: should be replaced by {@link KademliaProtocol#cache}
	 * update: No, we don't replace it since we changed the caching strategy from issuer node to the node with the closest NodeId to the issued query.
	 * 			so we need to keep track of what search queries and results we've issued/received so far, in this searchResults variable.
	 */
	private TreeMap<BigInteger, Object> searchResults;
	/**
	 * Replicate this object by returning an identical copy.<br>
	 * It is called by the initializer and do not fill any particular field.
	 * 
	 * @return Object
	 */
	public Object clone() {
		KademliaProtocol dolly = new KademliaProtocol(KademliaProtocol.prefix);
		return dolly;
	}

	@Override
	public String toString() {
		return "KademliaProtocol{" + 
				"PAR_K='" + PAR_K + '\'' + 
				", PAR_ALPHA='" + PAR_ALPHA + '\'' + 
				", PAR_BITS='" + PAR_BITS + '\'' + 
				", transport=" + transport + 
				", tid=" + tid + 
				", kademliaid=" + kademliaid + 
				", nodeId=" + nodeId + 
				", routingTable=" + routingTable + 
				", sentMsg=" + sentMsg + 
				", findOp=" + allIssuedfindOps + 
				", storeMap=" + storeMap + 
				", storeCapacity=" + storeCapacity +
				", cacheCapacity=" + cacheCapacity +
				", cache=[" + cache.toString() + ']' +
				'}';
	}
	/**
	 * this useless constructor is for the message distribution process 
	 * 
	 */
	public KademliaProtocol (BigInteger nodeId) {
		this.nodeId = nodeId;
		routingTable = new RoutingTable();

		//sentMsg = new TreeMap<Long, Long>();

		//findOp = new LinkedHashMap<Long, FindOperation>();

		//storeMap = new TreeMap<>();
		//searchResults = new TreeMap<>();
		
		//receivedVals = new HashSet<String>();

		//findVals = new ArrayList<>();

		//storeTimesMap = new HashMap<>();

		//nodeSpace = new TreeMap<>();
	}
	/**
	 * Used only by the initializer when creating the prototype. Every other
	 * instance call CLONE to create the new object.
	 * 
	 * @param prefix String
	 */
	public KademliaProtocol(String prefix) {
		this.nodeId = null; // empty nodeId
		KademliaProtocol.prefix = prefix;

		_init();

		routingTable = new RoutingTable();

		sentMsg = new TreeMap<Long, Long>();

		allIssuedfindOps = new LinkedHashMap<Long, FindOperation>();

		storeMap = new TreeMap<>();
		searchResults = new TreeMap<>();
		cache = new LRUCache(cacheCapacity);
		
		receivedVals = new HashSet<String>();

		findVals = new ArrayList<>();

		storeTimesMap = new HashMap<>();

		nodeSpace = new TreeMap<>();

		// - Randomly allocate storage capacity to each node, one of the following three
		// int[] arr = { 100, 500, 1000 };
		// int[] arr = {500};
		int[] arr = {10000};
		int rand = (int) (Math.random() * arr.length);
		this.storeCapacity = arr[rand];

		tid = Configuration.getPid(prefix + "." + PAR_TRANSPORT);
	}

	/**
	 * This procedure is called only once and allow to initialize the internal state
	 * of KademliaProtocol. Every node shares the same configuration, so it is
	 * sufficient to call this routine once.
	 */
	private void _init() {
		// execute once
		if (_ALREADY_INSTALLED)
			return;

		// read parameters
		KademliaCommonConfig.K = Configuration.getInt(prefix + "." + PAR_K, KademliaCommonConfig.K);
		KademliaCommonConfig.ALPHA = Configuration.getInt(prefix + "." + PAR_ALPHA, KademliaCommonConfig.ALPHA);
		KademliaCommonConfig.BITS = Configuration.getInt(prefix + "." + PAR_BITS, KademliaCommonConfig.BITS);
		
		cacheCapacity = Configuration.getInt(prefix + "." + PAR_CACHE, 0);
		_ALREADY_INSTALLED = true;
	}
	
	/**
	 * storeCapacity getter & setter added for message distribution of dataset from StoreMessageGenerator
	 * @return
	 */
	public int getStoreCapacity() {
		return storeCapacity;
	}

	public void setStoreCapacity(int storeCapacity) {
		this.storeCapacity = storeCapacity;
	}
	
	/**
	 * storeMap getter & setter added for message distribution of dataset from StoreMessageGenerator
	 * @return
	 */
	public TreeMap<BigInteger, Object> getStoreMap() {
		return storeMap;
	}
	/**
	 * storeMap getter & setter added for message distribution of dataset from StoreMessageGenerator
	 * @return
	 */
	public void setStoreMap(BigInteger key, Object val) {
		storeMap.put(key, val);
	}
	/**
	 * Search through the network the Node having a specific node Id, by performing
	 * binary search (we concern about the ordering of the network). Finding a node
	 * by binary search
	 * 
	 * @param searchNodeId BigInteger
	 * @return Node
	 */
	private Node nodeIdtoNode(BigInteger searchNodeId) {
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

	/**
	 * Continue to route after receiving the resp of the route Perform the required
	 * operation upon receiving a message in response to a ROUTE message.<br>
	 * Update the find operation record with the closest set of neighbor received.
	 * Then, send as many ROUTE request possible (according to the ALPHA
	 * parameter).<br>
	 * If no closest neighbor available and no outstanding messages stop the find
	 * operation.
	 *
	 * @param m     Message
	 * @param myPid the sender Pid
	 */
	private void route(Message m, int myPid) {
		// add message source to my routing table
		if (m.src != null) {
			routingTable.addNeighbour(m.src);// Add the node that sent resp to the routing table
		}

		// get corresponding find operation (using the message field operationId)
		// - Get fop, each node maintains a fop map, which is equivalent to a transfer
		// process, the nodes are consistent in the process
		FindOperation fop = this.allIssuedfindOps.get(m.operationId);

		if (fop != null) {
			// save received neighbor in the closest Set of find operation
			try {
				// - k nodes in the m.body are known to be the nearest node to the target. Use
				// these nodes to update the closeSet of the fop.
				fop.elaborateResponse((BigInteger[]) m.body);
			} catch (Exception ex) {
				fop.available_requests++;
			}

			while (fop.available_requests > 0) { // I can send a new find request -Can also send routing requests

				// get an available neighbor
				BigInteger neighbour = fop.getNeighbour();

				if (neighbour != null) {
					// create a new request to send to neighbor
					Message request = new Message(Message.MSG_ROUTE);
					request.operationId = m.operationId;
					request.src = this.nodeId;
					request.dest = m.dest;
					//System.out.println(m.body);
					//System.out.println(m.body instanceof BigInteger);
//					if(fop.body instanceof BigInteger) {
//						System.out.println("HHHHHHHHHHHHHHHHHHHHHHHHHHH");
//						System.out.println(((BigInteger[])m.body).length);
//						for (BigInteger arrElement : (BigInteger[])m.body) {
//
//				            System.out.println("Item: " + arrElement);
//				        }
//						System.out.println(m.body.getClass().getName());
//						System.out.println(fop.body.getClass().getName());
//						System.out.println(fop.body);
//						System.out.println(request.body.getClass().getName());
//						System.exit(1);
//					}
					
					// increment hop count
					fop.nrHops++;
					
					// send find request
					sendMessage(request, neighbour, myPid);
				} else if (fop.available_requests == KademliaCommonConfig.ALPHA) { // no new neighbor and no outstanding
																					// requests
					
					/* 24 SEP 
					I have commented out below remove operation bcz first of all I need the 
					prev ops to stay so that I merge multiple keyword search parts.
					secondly there was no different between removing it or leaving it in the term of cachehit or findmsg
					*/
					// search operation finished
					//allIssuedfindOps.remove(fop.operationId);
					
					// - Randomly generated FIND_NODE message
					if (fop.body.equals("Automatically Generated Traffic")
							&& fop.closestSet.containsKey(fop.destNode)) {
						// update statistics
						long timeInterval = (CommonState.getTime()) - (fop.timestamp);
						KademliaObserver.timeStore.add(timeInterval);
						KademliaObserver.hopStore.add(fop.nrHops);
						KademliaObserver.msg_deliv.add(1);
					} else if (fop.body instanceof StoreFile) { // store the kv in the closest nodes set
						for (BigInteger node : fop.closestSet.keySet()) {
							Message storeSpaceReqMsg = new Message(Message.MSG_STORE_SPACE_REQ, fop.body);
							storeSpaceReqMsg.src = this.nodeId;
							storeSpaceReqMsg.dest = node;
							storeSpaceReqMsg.operationId = m.operationId;
							// for findnode prev condition is finishing line, but for msg_store we still have to exchange msgs
							fop.nrHops++;
							sendMessage(storeSpaceReqMsg, node, myPid);
							
							// XXX - the stdout is only for debugging
							// System.out.println("send space ask msg to node:" + node);
						}
					} else if (fop.body instanceof BigInteger) { // ask the closest nodes for a value of the requested key.
//						System.out.println(fop.body);
//						System.out.println(fop.body instanceof BigInteger);
//						//m.body = new BigInteger[2];
//						System.out.println(((BigInteger[])m.body).length);
//						System.out.println(m.body);
//						System.exit(1);
						for (BigInteger node : fop.closestSet.keySet()) {
							Message findValMsg = new Message(Message.MSG_FINDVALUE, fop.body);
							// Message findValMsg = new Message(Message.MSG_FINDVALUE, "HELLLOOOOO");
							findValMsg.src = this.nodeId;
							findValMsg.dest = node;
							findValMsg.operationId = m.operationId;
							findValMsg.timestamp = fop.timestamp; 
							// there is one more sendMessge from this src to dest node so increase nrHops
							fop.nrHops++;
							//long timeInterval = (CommonState.getTime()) - (fop.timestamp);
							//if(timeInterval>2000)
							//System.out.println( ++KademliaObserver.h + "- " + timeInterval + " : " + fop.nrHops);
							// KademliaObserver.queryMsgTime.add(timeInterval);
							// FIXME the time interval is per search or per msg?? if per search then you should not count it here, rather it should be
							// counted when receiving the first valid answer for my search in receiveVal() 
							KademliaObserver.hopFindValue.add(fop.nrHops);
							sendMessage(findValMsg, node, myPid);
						}
					} /*
						 * else { System.out.println("Message got f***** up");
						 * System.out.println("simple m.body name: " +
						 * fop.body.getClass().getSimpleName()); System.out.println("name of m.body: " +
						 * fop.body.getClass().getName()); System.out.println("m.body value: " +
						 * fop.body); }
						 */

					return;

				} else { // no neighbor available but exists outstanding request to wait
					if (fop.body instanceof BigInteger) {
						//System.out.print("outstanding messages: " );
						//System.out.println(KademliaCommonConfig.ALPHA - fop.available_requests);
						// ++KademliaObserver.h;
					}
					return;
				}
			}
		} else {
			// in my case this is not error but its when cache responds the request
			// System.err.println("There has been some error in the protocol");
			
			//XXX 24-9 I don't know why or in what situation I get the following error!
//			System.err.println("the fop is not found in allIssued Operations");
//			System.out.println(m);
//			System.out.println(allIssuedfindOps);
//			System.out.println("\n");
			//System.exit(1);
		}
	}

	/**
	 * Response to a route request.<br>
	 * Find the ALPHA closest node consulting the k-buckets and return them to the
	 * sender. -Returns the K known nodes closest to the target node<br>
	 * sends back a message of type MSG_RESPONSE
	 * @param m     Message
	 * @param myPid the sender Pid
	 * 
	 */
	private void routeResponse(Message m, int myPid) {
		
		/*
		 * in case of FINDVAL process 
		 * before sending back the closest neighbors to the m.src, check your cache if you already have the desired data
		 * 
		 */
		//++KademliaObserver.h;
		/*
		 * BigInteger x = null; String gg = this.nodeId.toString(); int iii = 1; if
		 * (m.body instanceof BigInteger) { x = new
		 * BigInteger("681894339409086306801235361385093266063684137389", 10);
		 * System.out.println(KademliaObserver.h+" - "+ gg);
		 * if(gg.equalsIgnoreCase("681894339409086306801235361385093266063684137389")) {
		 * iii = 2; System.out.println(iii); } }
		 */
		
		/*
		 * checking its own storage and cache, if kv was available then return it. why
		 * storage check: bcz maybe I pinned the element to solve the bottleneck problem
		 * on actual provider
		 */
		boolean locallyAvailable = false;
		if (m.body instanceof BigInteger) {
			BigInteger key = (BigInteger) m.body;
			/*if (this.storeMap.containsKey(key) && this.cache.member(key)) {
				++KademliaObserver.h;
				// System.out.println(KademliaObserver.h);
			}*/
			/*if(this.storeMap.containsKey(key)) {
				//locallyAvailable = true;
				KademliaObserver.storageHit.add(1);
				//sendValue(m, myPid);
				//return;
			} else*/
			if(this.cache.member(key)){
				locallyAvailable = true;
				KademliaObserver.cacheHitPerMsg.add(1);
				sendValueFromCache(m, myPid);
				//System.exit(22);
				return;
			}
		} 
		if (!locallyAvailable) {
			KademliaObserver.cacheMissPerMsg.add(1);
			// get the ALPHA closest node to destNode
			// - Returns the K known nodes closest to the target node
			BigInteger[] neighbours = this.routingTable.getNeighbours(m.dest, m.src);
	
			// create a response message containing the neighbors (with the same id of the
			// request)
			Message response = new Message(Message.MSG_RESPONSE, neighbours);// The known k nearest node returns to the node
																				// src
			response.operationId = m.operationId;
			response.dest = m.dest;
			response.src = this.nodeId;
			response.ackId = m.id; // set ACK number
	
			// send back the neighbors to the source of the message
			sendMessage(response, m.src, myPid);
		}
	}
	
	/**
	 * Start a find node operation. Find the ALPHA closest node and send find
	 * request to them.
	 *
	 * @param m     Message received (contains the node to find)
	 * @param myPid the sender Pid
	 */
	private void find(Message m, int myPid) {

		if (m.type == Message.MSG_FINDNODE) { // increase find_op only when message type matches
			KademliaObserver.find_op.add(1);
		}
		// repeat below process by the number of keywords in the query
		// create find operation and add to operations array
		FindOperation fop = new FindOperation(m.dest, m.timestamp);
		fop.body = m.body;
		allIssuedfindOps.put(fop.operationId, fop);
		
		// get the ALPHA closest node to srcNode and add to find operation
		// get up to K closest nodes to the srcNode (or to a key) and add to find operation
		BigInteger[] neighbours = this.routingTable.getNeighbours(m.dest, this.nodeId);
		fop.elaborateResponse(neighbours);
		fop.available_requests = KademliaCommonConfig.ALPHA;
		
		// set message operation id，
		// - Forward information
		m.operationId = fop.operationId;
		m.type = Message.MSG_ROUTE;
		m.src = this.nodeId;
		//System.out.println(m.body);
		// send ALPHA messages
		for (int i = 0; i < KademliaCommonConfig.ALPHA; i++) {
			BigInteger nextNode = fop.getNeighbour(); // get the first neighbor in closest set which has not been
														// already queried
			if (nextNode != null) {
				sendMessage(m.copy(), nextNode, myPid);
				fop.nrHops++;
			}
			//System.out.println(m.copy());
			/*
			try(PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter("out.txt", true)))) {
			    out.println(m.copy().toString()+"------------------------------\n\n");
			}catch (IOException e) {
			    System.err.println(e);
			}*/
		}
		
	}
	/**
	 * Start a find value operation (query operation). Find the ALPHA closest node for each keyword and send find
	 * request to them.
	 *
	 * @param m     Message received (contains the query)
	 * @param myPid the sender Pid
	 */
	private void findValue(Message m, int myPid) {
		
		// repeat below process by the number of keywords in the query
		// create find operations and add them to the allIssuedOperations array
		BigInteger[] mBodyArr = (BigInteger[]) m.body;
		
		//System.out.println(mBodyArr.length);
		//System.out.println(mBodyArr[0]);
		//System.exit(1);
		/**
		 * check whether the requested key is cached by someone or not?
		 */
		
		//if(KademliaObserver.staticHashMap.containsKey(m.dest))
		//if(false)
		if(KademliaObserver.staticHashTable.contains(m.dest.toString()))
		{
			
			/*
			 * I can say m.body = m.dest
			 * but I wanted to create an independent object not a mere reference.
			 */
			m.body = new BigInteger(m.dest.toString());
			//System.out.println(m.body);
			//System.out.println(m.dest);
			//System.out.println(((BigInteger)m.body).equals(((BigInteger)m.dest)));
			find(m, myPid);
		}else {
			// TODO -- below is a dirty work, only works for query with 3 keywords, Should be changed to be more dynamic
			
			//2 keys have only one combination which is dividing it into singular keys, so only process it if >2
			if(mBodyArr.length>2) {
			// if(false) {
				// get all combinations to check for partial results
				Set<TreeSet<BigInteger>> combinations = PowerSet.combinations((BigInteger[])m.body, 2);
				// System.out.println(combinations);
				TreeMap<BigInteger, TreeSet<BigInteger>> inCache = new TreeMap<BigInteger, TreeSet<BigInteger>>();
				// look into each set to find out if any set has already been queried and cached?
				for(TreeSet<BigInteger> set : combinations) {
					BigInteger multikeyHash = null;
					String multikey = "";
					// go through elements of the set
					for(BigInteger elem : set) {
						multikey += elem.toString();
					}
					try {
						// generate the conjunctive keywords hash
						multikeyHash = new BigInteger(SHA1.shaEncode(multikey), 16);
					} catch (Exception e) {
						e.printStackTrace();
					}
					// check against cuckoo hash table 
					if(KademliaObserver.staticHashTable.contains(multikeyHash.toString()))
					{
						// store the sets that are available in in cache
						inCache.put(multikeyHash, set);
					}
				}
				int s = inCache.size();
				/** if there is at least one cached query, proceed to find the missing keyword then
				 *  create FOP for the cached keywords plus missing one.
				 *  i.e. k1+k2+k3 is query, k1k2 cached by some node. So create FOP for k1k2 + k3 
				 */
				if(s>0) {
					// 2 or 3 sets are in cache (since we only consider queries of 3 keywords)
					if(s>=2) {
						BigInteger[] cachedKeys = (BigInteger[]) inCache.keySet().toArray(new BigInteger[s]);
						// no reason to select first and second cached sets for FOP ! it just doesn't matter.
						//FIXME -- what if one or both of the requests were for deleted caches, that means there should be 
						// further check to make sure results were returned successfully otherwise create direct request for keywrods 
						createFop(new BigInteger[]{cachedKeys[0], cachedKeys[1]}, m, myPid);
					}else {
						// only one in cache
						TreeSet<BigInteger> collecter = inCache.get(inCache.firstKey());
						BigInteger missedE = null;
						for(BigInteger element : mBodyArr) {
							if(!collecter.contains(element)) {
								missedE = element;
								break;
							}
						}
						// System.out.println(missedE);
						if(missedE == null) {
							System.out.println(inCache);
							System.out.println(Arrays.toString(mBodyArr));
							System.exit(4);
						}
						createFop(new BigInteger[]{inCache.firstKey(), missedE}, m, myPid);
					}
				}else {
					// when non of the combinations have cache corresponds, then just create seperate msg for each of the keywords
					createFop(mBodyArr, m, myPid);
				}
			}else {
				// divide it into singular keys
				createFop(mBodyArr, m, myPid);
			}
		}
	}

	private void createFop(BigInteger[] mBodyArr, Message m, int myPid) {
		for(int i=0, msgLength=mBodyArr.length; i<msgLength; i++) {
			FindOperation fop = new FindOperation(mBodyArr[i], m.timestamp);
			fop.body = mBodyArr[i];
			fop.keywords = mBodyArr;
			if(msgLength>1) {
				// m.dest contains the combined keyword hash
				fop.parent = m.dest;
			}
			
			allIssuedfindOps.put(fop.operationId, fop);

			// get the ALPHA closest node to srcNode and add to find operation
			// get up to K closest nodes to the srcNode (or to a key) and add to find operation
			BigInteger[] neighbours = this.routingTable.getNeighbours(mBodyArr[i], this.nodeId);
			fop.elaborateResponse(neighbours);
			fop.available_requests = KademliaCommonConfig.ALPHA;
			
			// set message operation id，
			// - Forward information
			m.operationId = fop.operationId;
			m.type = Message.MSG_ROUTE;
			m.src = this.nodeId;

			// send ALPHA messages
			for (int a = 0; a < KademliaCommonConfig.ALPHA; a++) {
				BigInteger nextNode = fop.getNeighbour(); // get the first neighbor in closest set which has not been
														// already queried
				Message pendingMessage = null;
				if (nextNode != null) {
					pendingMessage = m.copy();
					pendingMessage.body = mBodyArr[i];
					pendingMessage.dest = mBodyArr[i];
					sendMessage(pendingMessage, nextNode, myPid);
					fop.nrHops++;
				}
				
				//System.out.println(pendingMessage);
				/*try(PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter("out2.txt", true)))) {
				    out.println(pendingMessage+"------------------------------\n\n");
				}catch (IOException e) {
				    System.err.println(e);
				}*/
			}
		}
		
	}

	/**
	 * Store the recieved value to kv
	 * 
	 * @param m     Message received (StoreFile object. Contains the key: item key,
	 *              value: ID of providers, and Size information)
	 * @param myPid The Sender Pid
	 */
	private void store(Message m, int myPid) {
		StoreFile sf = (StoreFile) m.body;
		boolean storedSucceed = false;
		if (this.storeCapacity >= sf.getSize()) {
			this.storeMap.put(sf.getKey(), sf.getValue());
			// XXX - the stdout is only for debugging
			// System.out.println("Node:" + this.nodeId + "(" + this.storeCapacity + "-" + sf.getSize() + ")"
					// + " storing kv data:" + sf.toString());
			this.storeCapacity = this.storeCapacity - sf.getSize();

			KademliaObserver.real_store_operation.add(1);
			storedSucceed = true;
		} else {
			// there should be some reponse to the rquester node that the store has failed,
			// I add storedSucceed=false
			// XXX - the stdout is only for debugging
			// System.out.println("Node:" + this.nodeId + ":" + this.storeCapacity
					// + " space storage is not enough to store kv data:" + sf.toString());

			storedSucceed = false;
			// I have added this observer
			KademliaObserver.real_store_fail_operation.add(1);
		}
		String respBody = sf.getKey() + "-" + storedSucceed;
		Message storeRespMsg = new Message(Message.MSG_STORE_RESP, respBody);
		storeRespMsg.src = this.nodeId;
		storeRespMsg.dest = m.src;
		storeRespMsg.operationId = m.operationId;
		KademliaObserver.sendstore_resp.add(1);
		sendMessage(storeRespMsg, m.src, myPid);

	}

	private void getStoreResp(Message m, int myPid) {
		String resp = (String) m.body;
		BigInteger key = new BigInteger(resp.split("-")[0]);
		boolean isSucceed = Boolean.parseBoolean(resp.split("-")[1]);
//		System.out.println(this.storeTimesMap.keySet().contains(key) );

		if (this.storeTimesMap.keySet().contains(key) && isSucceed) {
			int times = storeTimesMap.get(key);

			storeTimesMap.put(key, ++times);

			if (times > 0 && !storeSucceedFlag) {
				storeSucceedFlag = true;
				KademliaObserver.stored_msg.add(1);
				StoreMessageGenerator.generatedKeyList_status.put(key, storeSucceedFlag);

			}

		} else if(!storeSucceedFlag) {
			KademliaObserver.notstored_msg.add(1);
		}
	}

	/**
	 * respond the MSG_STORE_SPACE_REQ by sending back a message containing
	 * available storage size: by creating a new StoreFile object containing the
	 * same info of the received sf in the message. then update the
	 * NodeRemainingSize field and send back message as MSG_STORE_SPACE_RESP
	 * 
	 * @param m     received message about available storage size
	 * @param myPid involved protocol
	 */
	private void returnSpace(Message m, int myPid) {

		StoreFile sf = new StoreFile(((StoreFile) m.body).getKey(), ((StoreFile) m.body).getValue());
		sf.setSize(((StoreFile) m.body).getSize());
		sf.setStoreNodeRemainSize(this.storeCapacity);
		Message spaceRespMsg = new Message(Message.MSG_STORE_SPACE_RESP, sf);
		spaceRespMsg.src = this.nodeId;
		spaceRespMsg.dest = m.src;
		spaceRespMsg.operationId = m.operationId;
		sendMessage(spaceRespMsg, m.src, myPid);
		// XXX - the stdout is only for debugging
		// System.out.println(
				// "node:" + this.nodeId + " available space:" + sf.getStoreNodeRemainSize() + " to key: " + sf.getKey());
	}

	/**
	 * Sort the list of closest nodes to a key by space and then send store request
	 * to each of them.
	 * 
	 * @param m     the received message that contains the kv and available storage.
	 * @param myPid the corresponding protocol
	 */
	private void sortBySpaceAndSendStore(Message m, int myPid) {
		// FindOperation fop = this.findOp.get(m.operationId);
		// XXX - the stdout is only for debugging
		// System.out.println("storemsg requester node:" + this.nodeId + " received available space:" + ((StoreFile) m.body).getStoreNodeRemainSize() + " from: " + m.src);
		this.nodeSpace.put(m.src, ((StoreFile) m.body).getStoreNodeRemainSize());
		if (this.nodeSpace.size() >= KademliaCommonConfig.K) { // wait until all space request msgs are returned, then
																// sort and send store request message
			List<Map.Entry<BigInteger, Integer>> list = new ArrayList<>(nodeSpace.entrySet());
			// Sort by comparator
			Collections.sort(list, (o1, o2) -> {
				// Descending order
				return o2.getValue().compareTo(o1.getValue());
			});
//			for (Map.Entry<BigInteger, Integer> mapping : list) {
//				System.out.println(mapping.getKey() + ":" + mapping.getValue());
//			}
			// int i = 3; //why? it should be K as long as K is the replication parameter.
			int i = KademliaCommonConfig.K;
			for (Map.Entry<BigInteger, Integer> nodeMap : list) {
				/*
				 * currently the receiver will check its own storage, and respond with
				 * succeed=true or succeed=false and adding failed store operation to the
				 * observer if(nodeMap.getValue()< ((StoreFile)m.body).getSize()) { // skip the
				 * node if available storage is less than what is enough continue; }
				 */

				StoreFile sf = new StoreFile(((StoreFile) m.body).getKey(), ((StoreFile) m.body).getValue());

				sf.setStoreNodeRemainSize(nodeMap.getValue()); // m.body is the message of the last returned node so
				// it contains that nodes' info. here we reset the storage space remaining info
				// to the original node
				sf.setSize(((StoreFile) m.body).getSize());
				Message storeMsg = new Message(Message.MSG_STORE, sf);
				storeMsg.src = this.nodeId;
				storeMsg.dest = nodeMap.getKey();
				storeMsg.operationId = m.operationId;
				sendMessage(storeMsg, nodeMap.getKey(), myPid);
				if (--i == 0)
					break;
			}
			this.nodeSpace.clear();
		}
	}
	/**
	 * return queried keys' value if available
	 * @param m - query messages
	 * @param myPid
	 */
	@SuppressWarnings(value = { "unchecked" })
	private void sendValue(Message m, int myPid) {
		
		//TODO - followiong observer value mustn't be increased in case of premature val return (as I check in #routResponse())
		KademliaObserver.closeNodeValExpected.add(1);
		BigInteger key = (BigInteger) m.body;
		// System.out.println(this.nodeId);
		// BigInteger key = new BigInteger( ((BigInteger)m.body).toString(10), 10);
		boolean valueExist = false;
		for (BigInteger k : this.storeMap.keySet()) {
			if (k.equals(key)) {
				valueExist = true;
				Set<String> values = new HashSet<String>((Set<String>)this.storeMap.get(key));
				
				ArrayList<Object> msg_body = new ArrayList<>();
				msg_body.add(key);
				msg_body.add(values);
				Message returnValMsg = new Message(Message.MSG_RETURNVALUE, null);
				returnValMsg.src = this.nodeId;
				returnValMsg.dest = m.src;
				returnValMsg.operationId = m.operationId;
				returnValMsg.body = msg_body;
				returnValMsg.timestamp = m.timestamp;
//				if(k.equals(new BigInteger("44553308054427856964403383806978503551424428757"))) {
//					System.out.println(values + " results for empty return");
//					System.out.println(returnValMsg.dest);
//				}
				
				// XXX - the stdout is only for debugging
				// System.out.println("node:" + nodeId + " return value " + val + " to node:" + m.src);
				
				sendMessage(returnValMsg, m.src, myPid);
				KademliaObserver.closeNodeHadVal.add(1);
				break;
			}
		}
		/*if(!valueExist) {
			System.out.println(m.body);
			Message returnValMsg = new Message(Message.MSG_RETURNVALUE, null);
			returnValMsg.src = this.nodeId;
			returnValMsg.dest = m.src;
			returnValMsg.operationId = m.operationId;
			returnValMsg.body = "No Value for the requested query";
			returnValMsg.timestamp = m.timestamp;
			sendMessage(returnValMsg, m.src, myPid);
			KademliaObserver.closeNodeNoVal.add(1);
		}*/
	}
	
	/**
	 * return queried kv from cache (cache hit)
	 * 
	 */
	@SuppressWarnings(value = { "unchecked" })
	private void sendValueFromCache(Message m, int myPid) {
		
		// KademliaObserver.closeNodeValExpected.add(1);
		BigInteger key = (BigInteger) m.body;
		// System.out.println(this.nodeId);
		// BigInteger key = new BigInteger( ((BigInteger)m.body).toString(10), 10);
		for (BigInteger k : this.cache.allKeys()) {
			if (k.equals(key)) {
				Set<String> values = new HashSet<String>((Set<String>)this.cache.get(key));
				ArrayList<Object> msg_body = new ArrayList<>();
				msg_body.add(key);
				msg_body.add(values);
				Message returnValMsg = new Message(Message.MSG_RETURNVALUE_FROM_CACHE, null);
				//Message returnValMsg = new Message(Message.MSG_RETURNVALUE, null);
				returnValMsg.src = this.nodeId;
				returnValMsg.dest = m.src;
				returnValMsg.operationId = m.operationId;
				returnValMsg.body = msg_body;
				returnValMsg.ackId = m.ackId;
				// XXX - the stdout is only for debugging
				// System.out.println("////////node:" + nodeId + " return value " + values + " to node:" + m.src);
				sendMessage(returnValMsg, m.src, myPid);
				// KademliaObserver.closeNodeHadVal.add(1);
				break;
			}
		}
	}
	/**
	 * receive the result of your issued query from corresponding node
	 * NOTE: IF NO RESULT FOUND, THIS METHOD WON'T BE CALLED, IT STOPS BEFORE GETTING INTO HERE, in {@link #sendValue(Message, int)}
	 * @param m
	 * @param myPid
	 */
	@SuppressWarnings(value = { "unchecked" })
	private void receiveVal(Message m, int myPid) {
		
		// if no the queried node has no data about the requested query
		if(m.body instanceof String) {
			//System.out.println(m.body);
			FindOperation fop = this.allIssuedfindOps.get(m.operationId);
			if (fop != null) {
				// deleteResultInCache(m.key);
				//System.exit(1);
				/**
				 * 1- lawanaya la 20 request chand danayakian no result bet w handeki dikayan resultian habet, chi dakai awkat?
				 * 2- boya this is not a good solution, better remove from cuckoo
				 * 3- cuckoo false posetive i haya, wata lawanaya nabe w ble haya, awkat chi akai???
				 * 4- dabi ba shewayak am noda bzanet bo am queria chand outstanding msg i haya
				 * 		kai outstanding msg bu ba zero awkat amai xwarawa bkat
				 * 5- ba haman shewash remove krdn la cuckoo keshai haya, bo nmuna nodek remove i krd lawanaya awani tr heshta hayanbet,
				 * 		so agar la cuckoo remove bkain awkat wak awa waya blein kas niati
				 * 		sarbari awash datwanin blein awai remove dakre is the least important key so its safe to remove altogether
				 */
				KademliaObserver.findVal_success.add(-1);
				long timeInterval = (CommonState.getTime()) - (m.timestamp);
				// System.out.println( ++KademliaObserver.h + "- " + timeInterval + " : " + m.nrHops);
				KademliaObserver.queryMsgTime.add(timeInterval);
			}
			return;
		}
		
		// fop.nrHops++; // wam birkrdawa ka dabe zyad bkre, bas naxer chunka nrhops bas zhmarai aw msganaya ka src node dainere
		
		//long timeInterval1 = (CommonState.getTime()) - (m.timestamp);
		//System.out.println( ++KademliaObserver.h + "- " + timeInterval1 + " : " + m.nrHops);
		BigInteger receKey = ((ArrayList<BigInteger>) m.body).get(0);
		Set<String> receVal = ((ArrayList<HashSet<String>>) m.body).get(1);
		
		/**
		 * the condition below is to make sure we count the received value one time since we may receive results of the issued query from 
		 * different nodes (because findValue is parallel).
		 * //if(searchResults.get(receKey) == null || !((Set<String>)searchResults.get(receKey)).containsAll(receVal)) {
		 */
		// TODO -- what if the same node with the same query to issue, is chosen again by the simulator, the received value won't be counted if results are exactly the same
		// and the findValuesuccess won't be increased even if we get the value for the issued query, I have to fix this in some ways
		
		if(searchResults.get(receKey) == null) {
			searchResults.put(receKey, receVal);
			
			//------
			FindOperation fop = this.allIssuedfindOps.get(m.operationId);
			if (fop != null) {
				allIssuedfindOps.remove(m.operationId);
				// if search was multikeyword
				if(fop.parent != null) {
					Set<String> allValues = null;
					// loop through all the requested searches of fop.keywords, if we have results for all of them then finish the search
					boolean searchFinished = true;
					for(BigInteger key: fop.keywords) {
						if(searchResults.get(key) == null) {
							searchFinished = false;
						}
					}
					
					if(searchFinished) {
						KademliaObserver.h++;
						//allValues = (HashSet<String>) searchResults.get(fop.keywords[0]); //creates a reference not new obj
						allValues = new HashSet<String>((Set<String>) searchResults.get(fop.keywords[0]));
						for(int x=1; x<fop.keywords.length; x++) { 
							allValues.retainAll((HashSet<String>) searchResults.get(fop.keywords[x]));
						}
						/*if(!allValues.isEmpty()) {
							//System.out.println(allValues);
							//System.out.println("Fisnish");
							//System.exit(1);
							KademliaObserver.h++;
						}*/
						searchResults.put(fop.parent, allValues);
						storeResultInCache(fop.parent, allValues);
						KademliaObserver.findVal_success.add(1);
						long timeInterval = (CommonState.getTime()) - (m.timestamp);
						KademliaObserver.queryMsgTime.add(timeInterval);
//						System.out.println(fop);
//						System.out.println("----------------- "+ this.nodeId);
//						System.out.println(searchResults);
//						System.out.println("-----------------");
//						System.out.println(searchResults.get(fop.keywords[0]));
//						System.out.println("-----------------");
						//System.exit(1);
					
					}
					
				}else {
					storeResultInCache(receKey, receVal);
					//System.exit(1);
					KademliaObserver.findVal_success.add(1);
					long timeInterval = (CommonState.getTime()) - (m.timestamp);
					// System.out.println( ++KademliaObserver.h + "- " + timeInterval + " : " + m.nrHops);
					KademliaObserver.queryMsgTime.add(timeInterval);
				}
			}
			
		}
	}
	
	/**
	 * Cache the search result in the node with the closest NodeId to the issued query directly 
	 * without following proper DHT steps bcz we are not interested in knowing the bandwidth cost or time cost of this process.
	 * @param receKey <br>The searched keyword
	 * @param receVal <br>The received result set
	 */static int maxSize = 200; // max number of cache when 2 keywords used in queries raised to 2524 when [2-3] keywords are used
	public void storeResultInCache(BigInteger receKey, Set<String> receVal) {
		this.cache.set(receKey, receVal);
		BigInteger[] kClosestNodeIds = KademliaObserver.supernode.routingTable.getNeighbours2(receKey, KademliaObserver.supernode.getNodeId());
		
		for (BigInteger closeNodeId : kClosestNodeIds) {
			Node tmp = nodeIdtoNode(closeNodeId);
			if(!tmp.isUp()) {
				continue;
			}
			KademliaProtocol closeNodeKad = (KademliaProtocol) tmp.getProtocol(kademliaid);
			closeNodeKad.cache.set(receKey, receVal);
			BigInteger closeNodeLastRKey = closeNodeKad.cache.getLastRemovedKey();
			if(closeNodeLastRKey != null) {
				KademliaObserver.staticHashTable.remove(closeNodeLastRKey.toString());
				
			}
			if(closeNodeKad.cache.getSize()>maxSize) {
				maxSize=closeNodeKad.cache.getSize();
				System.out.print(maxSize+"-");
			}
			
			/*
			 * TODO - what we should do is something like this
			 * closNodeKad.cuckooHashMap.put(receKey, receVal.toString());
			 * then broadcast the update to all other nodes in the network by
			 * closNodeKad.broadcastHashMap();
			 * 
			 * public void broadcastHashMapUpdate() {
			 *  Message broadcast = makeHashMapUpdateRequest([recekey, receValeu]);
			 * 	sendMessage(broadcast, allNeighborNodes)
			 * }
			 * 
			 * public static final Message makeHashMapUpdateRequest(Object  body){
			 *	return new Message(MSG_BROADCASTHASHMAPUPDATE_REQ, body);
			 *}
			 */
		}
		// update the hash map to show that the query "recekey" is cached. 
		//KademliaObserver.staticHashMap.put(receKey, receVal.toString());
		KademliaObserver.staticHashTable.insert(receKey.toString());
	}

	/**
	 * receive the result of your issued query from corresponding node (result found in cache)
	 * @param m
	 * @param myPid
	 */
	@SuppressWarnings(value = { "unchecked" })
	private void receiveValFromCache(Message m, int myPid) {
		
		// fop.nrHops++; // wam birkrdawa ka dabe zyad bkre, bas naxer chunka nrhops bas zhmarai aw msganaya ka src node dainere
		// TODO change closes node to true for the src of this msg
		// FIXME if I do fop.available_requests++; without stopping the other ongoing searches, somehow the available_requests will be increased by 2 for msg 761, meaning available will become 5   
		// TODO maybe I have to stop all other ongoiing requests when I receive one result
		FindOperation fop = this.allIssuedfindOps.get(m.operationId);
		if (fop == null) { // I'm not sure! this happens when among ALPHA requests more than one has it in cache, so first node returns value faster and src node will remove the operation from issuedfindops, then it receives answer from second nodes cache 
							//(other than cache reponses, msgs will timeout)
			return;
		}
		fop.available_requests++;
		//this remove will make other ongoing searches useless and result in "something wrong with protocol" output
		this.allIssuedfindOps.remove(m.operationId);
		// TODO - nrHops should be moved to the inside if condition below I think, bcz we want to count if its the first result
		KademliaObserver.hopFindValue.add(fop.nrHops);
		BigInteger receKey = ((ArrayList<BigInteger>) m.body).get(0);
		Set<String> receVal = ((ArrayList<HashSet<String>>) m.body).get(1);
		if((Set<String>)searchResults.get(receKey) == null || !((Set<String>)searchResults.get(receKey)).containsAll(receVal)) {
		//if (!receivedVals.contains(receVal)) {
			searchResults.put(receKey, receVal);
			// System.exit(12);
			storeResultInCache(receKey, receVal);
			KademliaObserver.findVal_success.add(1);
			KademliaObserver.cacheHitPerQuery.add(1);
			long timeInterval = (CommonState.getTime()) - (fop.timestamp);
			// System.out.println( ++KademliaObserver.h + "- " + timeInterval + " : " + fop.nrHops);
			KademliaObserver.queryMsgTime.add(timeInterval);
			// XXX - the stdout is only for debugging
			// System.err.println("node " + this.nodeId + "'s findVals:" + this.searchResults);

			// System.err.println("node " + this.nodeId + "'s receiveVals:" + this.receivedVals);
		}
	}
	
	/**
	 * send a message with current transport layer and starting the timeout timer
	 * (which is an event) if the message is a request
	 *
	 * @param m      the message to send
	 * @param destId the Id of the destination node
	 * @param myPid  the sender Pid
	 */
	public void sendMessage(Message m, BigInteger destId, int myPid) {
		// add destination to routing table
		this.routingTable.addNeighbour(destId);

		Node src = nodeIdtoNode(this.nodeId);
		Node dest = nodeIdtoNode(destId);

		transport = (UnreliableTransport) (Network.prototype).getProtocol(tid);
		transport.send(src, dest, m, kademliaid);

		if (m.getType() == Message.MSG_ROUTE) { // is a request
			Timeout t = new Timeout(destId, m.id, m.operationId);
			long latency = transport.getLatency(src, dest);
			// System.out.println(latency*4);
			// add to sent msg
			this.sentMsg.put(m.id, m.timestamp);
			EDSimulator.add(4 * latency, t, src, myPid); // set delay = 2*RTT(Round trip time)
		}
	}

	/**
	 * manage the peersim receiving of the events
	 *
	 * @param myNode Node
	 * @param myPid  int
	 * @param event  Object
	 */
	public void processEvent(Node myNode, int myPid, Object event) {

		// Parse message content Activate the correct event manager fot the particular
		// event
		this.kademliaid = myPid;

		Message m;
		// System.out.println(this);
//		if((((KademliaProtocol) this).getNodeId().equals(new BigInteger("1436294760649089404056870643165375864014011356023")))){
//			KademliaProtocol kp = ((KademliaProtocol) (myNode.getProtocol(myPid)));
//			System.out.println(Message.messageTypetoString(((SimpleEvent) event).getType()));
//		}

		switch (((SimpleEvent) event).getType()) {

		case Message.MSG_FINDNODE:
			m = (Message) event;
			find(m, myPid);
			break;

		case Message.MSG_ROUTE:
			m = (Message) event;
			routeResponse(m, myPid);
			break;

		case Message.MSG_RESPONSE:
			m = (Message) event;
			sentMsg.remove(m.ackId);
			route(m, myPid);
			break;

		case Message.MSG_EMPTY:
			// TODO
			break;

		case Message.MSG_STORE_REQUEST: // case activated by StoreMessageGenerator
			m = (Message) event;
			// reset storeSucceedFlag to false, needed in case of if in prev request it
			// became true,
			// and now the same node issues another request, then the flag should be reseted
			// to false
			storeSucceedFlag = false;
			// XXX - the stdout is only for debugging
			// System.out.println("This node:" + this.getNodeId() + " got kv from generator :" + m.body);
			// System.out.println("the generateStore:" + Collections.frequency(StoreMessageGenerator.generatedKeyList_status.values(), true));

			// find closes nodes to the key
			find(m, myPid);

			this.storeTimesMap.put((BigInteger) ((StoreFile) m.body).getKey(), 0);

			KademliaObserver.sendtostore_msg.add(1);
			break;


		case Message.MSG_STORE_SPACE_REQ:
			m = (Message) event;
			returnSpace(m, myPid);
			break;

		case Message.MSG_STORE_SPACE_RESP:
			m = (Message) event;
			sortBySpaceAndSendStore(m, myPid);
			break;

		case Message.MSG_STORE:
			m = (Message) event;
			store(m, myPid);
			break;

		case Message.MSG_STORE_RESP:
			m = (Message) event;
			getStoreResp(m, myPid);
			break;

		case Message.MSG_FINDVALUE_REQ:
			m = (Message) event;
			//TODODONE - change searchResults to cache storage checking
			// XXX - the stdout is only for debugging
			//System.out.println("This node:" + this.nodeId + " finding value of key:" + m.body);
			/*
			 * if (!searchResults.containsKey((BigInteger) m.body) ||
			 * searchResults.get((BigInteger) m.body) == null ) {
			 * searchResults.put((BigInteger) m.body, null); find(m, myPid); // XXX - the
			 * stdout is only for debugging // System.err.println("Again This node:" +
			 * this.nodeId + "'s foundedVals:" + this.findVals+"\n");
			 * KademliaObserver.findVal_times.add(1); } else { //System.err.println("node "
			 * + this.nodeId + "'s findVals:" + this.searchResults);
			 * System.out.println("This node already has this value: " + m.body); }
			 */
			
			// `if` condition parameter was m.body but changed to m.dest bcz after adding the multikeyword capability
			// the m.body holds the array of keys while the m.dest holds the the combined keyword hash value
			if (!this.cache.member(m.dest) ) {
				searchResults.put(m.dest, null);
				// XXX - the stdout is only for debugging
				// System.err.println("Again This node:" + this.nodeId + "'s foundedVals:" + this.findVals+"\n");
				KademliaObserver.findVal_times.add(1);
				findValue(m, myPid);
				//find(m, myPid);
				
			} else {
				KademliaObserver.findVal_times.add(1);
				KademliaObserver.findVal_success.add(1);
				KademliaObserver.hopFindValue.add(1);
				long timeInterval = (CommonState.getTime()) - (m.timestamp);
				KademliaObserver.queryMsgTime.add(timeInterval);
				
				KademliaObserver.cacheHitPerMsg.add(1);
				KademliaObserver.cacheHitPerQuery.add(1);
				KademliaObserver.duplicateQuery.add(1);
				//System.out.println("available on local cache: " + m.dest);
			}
			break;

		case Message.MSG_FINDVALUE:
			m = (Message) event;
			sendValue(m, myPid);
			break;

		case Message.MSG_RETURNVALUE:
			m = (Message) event;
			receiveVal(m, myPid);
			break;
			
		case Message.MSG_RETURNVALUE_FROM_CACHE:
			m = (Message) event;
			sentMsg.remove(m.ackId);
			receiveValFromCache(m, myPid);
			break;

		case Timeout.TIMEOUT: // timeout
			Timeout t = (Timeout) event;
			if (sentMsg.containsKey(t.msgID)) { // the response msg isn't arrived
				// remove from sentMsg
				sentMsg.remove(t.msgID);
				// remove from closestSet of find operation if not already removed
				if(this.allIssuedfindOps.get(t.opID) != null) {
					
					this.allIssuedfindOps.get(t.opID).closestSet.remove(t.node);
					// remove node from my routing table
					this.routingTable.removeNeighbour(t.node);
					// try another node
					Message m1 = new Message();
					m1.operationId = t.opID;
					m1.src = nodeId;
					m1.dest = this.allIssuedfindOps.get(t.opID).destNode;
					this.route(m1, myPid);
				}
			}
			break;

		}

	}

	/**
	 * set the current NodeId
	 * 
	 * @param tmp BigInteger
	 */
	public void setNodeId(BigInteger tmp) {
		this.nodeId = tmp;
		this.routingTable.nodeId = tmp;
	}

	public BigInteger getNodeId() {
		return nodeId;
	}
}
