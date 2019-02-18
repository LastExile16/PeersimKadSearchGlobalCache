package peersim;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.omg.PortableInterceptor.INACTIVE;
import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Control;
import peersim.core.Network;
import peersim.util.IncrementalStats;

/**
 * This class implements a simple observer of search time and hop average in finding a node in the network
 * 
 * @author Daniele Furlan, Maurizio Bonani
 * @version 1.0
 */
public class KademliaObserver implements Control {
public static int h=0;
	/**
	 * keep statistics of the number of hops of every message delivered.
	 */
	public static IncrementalStats hopStore = new IncrementalStats();

	/**
	 * keep statistics of the time every message delivered.
	 */
	public static IncrementalStats timeStore = new IncrementalStats();

	/**
	 * keep statistic of number of message delivered
	 */
	public static IncrementalStats msg_deliv = new IncrementalStats();

	/**
	 * keep statistic of number of find operation
	 */
	public static IncrementalStats find_op = new IncrementalStats();

	/**
	 * keep statistic of number of successful store message, *Not number of times each message stored
	 * 
	 */
	public static IncrementalStats stored_msg = new IncrementalStats();
	
	/**
	 * statistics of number of successful store operations
	 */
	public static IncrementalStats real_store_operation = new IncrementalStats();


	/**
	 * keep statistic of the number of send store_msg request (MSG_STORE_REQUEST)
	 * 
	 */
	public static IncrementalStats sendtostore_msg = new IncrementalStats();
	
	/**
	 * statistics of number of msg responses to MSG_STORE, that is either succeeded or failed
	 */
	public static IncrementalStats sendstore_resp = new IncrementalStats();
	
	/**
	 * keep statistic of the number of failed store operations (this is per operation {i.e. per node})
	 */
	public static IncrementalStats real_store_fail_operation = new IncrementalStats();
	
	/**
	 * keep statistic of the number of not stored messages (this is per message) 
	 */
	public static IncrementalStats notstored_msg = new IncrementalStats();
	/**
	 * Number of overloaded nodes
	 */
	public static IncrementalStats overloadNode = new IncrementalStats();
	/**
	 * keep statistic of number of find value success,表示成功find value的次数
	 */
	public static IncrementalStats findVal_success  = new IncrementalStats();

	/**
	 * 发起find value的次数
	 * The number of initiating "find value"
	 */
	public static IncrementalStats findVal_times = new IncrementalStats();

	/**
	 * keep statistic of the number of times a node expected to have a value for the queried key (the node counted as close node to key)
	 */
	public static IncrementalStats closeNodeValExpected = new IncrementalStats();

	/**
	 * keep statistic of the number of times a node had the value and returned it (the close node had the value as expected)
	 */
	public static IncrementalStats closeNodeHadVal = new IncrementalStats();
	
	/** Parameter of the protocol we want to observe */
	private static final String PAR_PROT = "protocol";

	/** Protocol id */
	private int pid;

	/** Prefix to be printed in output */
	private String prefix;

	public KademliaObserver(String prefix) {
		this.prefix = prefix;
		pid = Configuration.getPid(prefix + "." + PAR_PROT);
	}

	/**
	 * print the statistical snapshot of the current situation
	 * 
	 * @return boolean always false
	 */
	public boolean execute() {
		// get the real network size
		int sz = Network.size();
		for (int i = 0; i < Network.size(); i++)
			if (!Network.get(i).isUp())
				sz--;

		/*String s = String.format("[time=%d]:[N=%d current nodes UP] [D=%f msg deliv] [%f min h] [%f average h] [%f max h] [%d min l] [%d msec average l] [%d max l] [%d findop sum] [%d sendstore_resp sum]  [%d storedMsg sum]  [%d sendtostore_msg sum] [%d findValueSuccess sum] [%d findValueTimes][%d realStoreOperation]",
				CommonState.getTime(), sz, msg_deliv.getSum(),hopStore.getMin(), hopStore.getAverage(), hopStore.getMax(), (int) timeStore.getMin(), (int) timeStore.getAverage(), (int) timeStore.getMax(),(int)find_op.getSum(),(int)sendstore_resp.getSum(),(int)stored_msg.getSum(),(int)sendtostore_msg.getSum(),(int)findVal_success.getSum(),(int)findVal_times.getSum(),(int)real_store_operation.getSum());
		*/
		/*String s = String.format("[time=%d]:[N=%d current nodes UP] [%d findop sum] [%d closeNodeValExpected sum]  [%d storedMsg sum]  [%d sendtostore_msg sum] [%d findValueSuccess sum] [%d findValueTimes][%d realStoreOperation]",
				CommonState.getTime(), sz,(int)find_op.getSum(),(int)closeNodeValExpected.getSum(),(int)stored_msg.getSum(),(int)sendtostore_msg.getSum(),(int)findVal_success.getSum(),(int)findVal_times.getSum(),(int)real_store_operation.getSum());
		*/
		String s = String.format("[time=%d]:[N=%d current nodes UP] [%d findop sum] [%d closeNodeValExpected sum]  [%d closeNodeHadVal sum]  [%d overloadNode sum] [%d findValueTimes sum] [%d findValueSuccess] [%d realStoreOperation] [%d realStoreFailOperation]",
				CommonState.getTime(), sz,(int)find_op.getSum(),(int)closeNodeValExpected.getSum(),(int)closeNodeHadVal.getSum(),(int)overloadNode.getSum(),(int)findVal_times.getSum(),(int)findVal_success.getSum(),(int)real_store_operation.getSum(), (int)real_store_fail_operation.getSum());
		
		if (CommonState.getTime() == 3600000) {
			// create hop file
			try {
				File f = new File("./hopcountNEW.dat"); // " + sz + "
				f.createNewFile();
				BufferedWriter out = new BufferedWriter(new FileWriter(f, true));
				out.write(String.valueOf(hopStore.getAverage()).replace(".", ",") + ";\n");
				out.close();
			} catch (IOException e) {
			}
			// create latency file
			try {
				File f = new File("./latencyNEW.dat");
				f.createNewFile();
				BufferedWriter out = new BufferedWriter(new FileWriter(f, true));
				out.write(String.valueOf(timeStore.getAverage()).replace(".", ",") + ";\n");
				out.close();
			} catch (IOException e) {
			}

		}

		System.err.println(s);

		return false;
	}
}
