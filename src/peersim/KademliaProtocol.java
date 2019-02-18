package peersim;

import java.io.IOException;

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

import joinery.DataFrame;
import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Network;
import peersim.core.Node;
import peersim.edsim.EDProtocol;
import peersim.edsim.EDSimulator;
import peersim.transport.UnreliableTransport;

//__________________________________________________________________________________________________
public class KademliaProtocol implements Cloneable, EDProtocol {

	// VARIABLE PARAMETERS
	final String PAR_K = "K";
	final String PAR_ALPHA = "ALPHA";
	final String PAR_BITS = "BITS";

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
	private LinkedHashMap<Long, FindOperation> findOp;

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
	 * node cachce capacity
	 */
	private int cacheCapacity = 50;
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
	 * FIXME should be replaced by {@link KademliaProtocol#cache}
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
		return "KademliaProtocol{" + "PAR_K='" + PAR_K + '\'' + ", PAR_ALPHA='" + PAR_ALPHA + '\'' + ", PAR_BITS='"
				+ PAR_BITS + '\'' + ", transport=" + transport + ", tid=" + tid + ", kademliaid=" + kademliaid
				+ ", nodeId=" + nodeId + ", routingTable=" + routingTable + ", sentMsg=" + sentMsg + ", findOp="
				+ findOp + ", storeMap=" + storeMap + ", storeCapacity=" + storeCapacity + '}';
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

		findOp = new LinkedHashMap<Long, FindOperation>();

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
	 * Than, send as many ROUTE request I can (according to the ALPHA
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
		FindOperation fop = this.findOp.get(m.operationId);

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

					// increment hop count
					fop.nrHops++;

					// send find request
					sendMessage(request, neighbour, myPid);
				} else if (fop.available_requests == KademliaCommonConfig.ALPHA) { // no new neighbor and no outstanding
																					// requests
					// search operation finished
					findOp.remove(fop.operationId);
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
							sendMessage(storeSpaceReqMsg, node, myPid);
							// XXX - the stdout is only for debugging
							// System.out.println("send space ask msg to node:" + node);
						}
					} else if (fop.body instanceof BigInteger) { // ask the closest nodes for a value of the requested key.
						for (BigInteger node : fop.closestSet.keySet()) {
							Message findValMsg = new Message(Message.MSG_FINDVALUE, fop.body);
							findValMsg.src = this.nodeId;
							findValMsg.dest = node;
							findValMsg.operationId = m.operationId;
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
					return;
				}
			}
		} else {
			System.err.println("There has been some error in the protocol");
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
		
		// TODO - make it check cache instead of storage
		if (m.body instanceof BigInteger) {
			BigInteger key = (BigInteger) m.body;
			if(this.storeMap.containsKey(key)) {
				sendValue(m, myPid);
			}
		}
		
		
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

		// create find operation and add to operations array
		FindOperation fop = new FindOperation(m.dest, m.timestamp);
		fop.body = m.body;
		findOp.put(fop.operationId, fop);

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

		// send ALPHA messages
		for (int i = 0; i < KademliaCommonConfig.ALPHA; i++) {
			BigInteger nextNode = fop.getNeighbour(); // get the first neighbor in closest set which has not been
														// already queried
			if (nextNode != null) {
				sendMessage(m.copy(), nextNode, myPid);
				fop.nrHops++;
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
	
	@SuppressWarnings(value = { "unchecked" })
	private void sendValue(Message m, int myPid) {
		KademliaObserver.closeNodeValExpected.add(1);
		BigInteger key = (BigInteger) m.body;
		//System.out.println(this.nodeId);
		// BigInteger key = new BigInteger( ((BigInteger)m.body).toString(10), 10);
		for (BigInteger k : this.storeMap.keySet()) {
			if (k.equals(key)) {
				Set<String> values = (HashSet<String>)this.storeMap.get(key);
				ArrayList<Object> msg_body = new ArrayList<>();
				msg_body.add(key);
				msg_body.add(values);
				Message returnValMsg = new Message(Message.MSG_RETURNVALUE, null);
				returnValMsg.src = this.nodeId;
				returnValMsg.dest = m.src;
				returnValMsg.operationId = m.operationId;
				returnValMsg.body = msg_body;
				// XXX - the stdout is only for debugging
				// System.out.println("node:" + nodeId + " return value " + val + " to node:" + m.src);
				sendMessage(returnValMsg, m.src, myPid);
				KademliaObserver.closeNodeHadVal.add(1);
				break;
			}
		}
	}
	/**
	 * receive the result of your issued query from corresponding node
	 * @param m
	 * @param myPid
	 */
	@SuppressWarnings(value = { "unchecked" })
	private void receiveVal(Message m, int myPid) {
		BigInteger receKey = ((ArrayList<BigInteger>) m.body).get(0);
		Set<String> receVal = ((ArrayList<HashSet<String>>) m.body).get(1);
		if((Set<String>)searchResults.get(receKey) == null || !((Set<String>)searchResults.get(receKey)).containsAll(receVal)) {
		//if (!receivedVals.contains(receVal)) {
			searchResults.put(receKey, receVal);
			cache.set(receKey, receVal);
			KademliaObserver.findVal_success.add(1);
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
			// XXX - the stdout is only for debugging
			System.out.println("This node:" + this.nodeId + " finding value of key:" + m.body);
			if (!searchResults.containsKey((BigInteger) m.body) || searchResults.get((BigInteger) m.body) == null ) {
				searchResults.put((BigInteger) m.body, null);
				find(m, myPid);
				// XXX - the stdout is only for debugging
				// System.err.println("Again This node:" + this.nodeId + "'s foundedVals:" + this.findVals+"\n");
				KademliaObserver.findVal_times.add(1);
			} else {
				System.err.println("node " + this.nodeId + "'s findVals:" + this.searchResults);
				System.out.println("This node already has this value: " + m.body);
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

		case Timeout.TIMEOUT: // timeout
			Timeout t = (Timeout) event;
			if (sentMsg.containsKey(t.msgID)) { // the response msg isn't arrived
				// remove form sentMsg
				sentMsg.remove(t.msgID);
				// remove node from my routing table
				this.routingTable.removeNeighbour(t.node);
				// remove from closestSet of find operation
				this.findOp.get(t.opID).closestSet.remove(t.node);
				// try another node
				Message m1 = new Message();
				m1.operationId = t.opID;
				m1.src = nodeId;
				m1.dest = this.findOp.get(t.opID).destNode;
				this.route(m1, myPid);
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
