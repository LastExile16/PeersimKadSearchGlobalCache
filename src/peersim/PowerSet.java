package peersim;

import java.math.BigInteger;
import java.util.Comparator;
import java.util.Set;
import java.util.TreeSet;
/**
 * A class to generate all possible subsets of a given set exculding the empty set and itself. 
 * @author Nawras Nazar
 *
 */
public class PowerSet {
	/**
	 * generate the powerset for the given set of keys<br>
	 * I exculded empty set and full set on purpose.
	 * @param key the set of generated keys
	 * @param minL minimum length of each subset
	 * @return a powerset of the given set
	 */
	public static Set<TreeSet<BigInteger>> combinations(BigInteger[] key, int minL)
	{
		// convert set to a list
		//List<Integer> S = new ArrayList<>(key);
		

		// N stores total number of subsets (2^size)
		long N = 1<<key.length;

		// Set to store subsets
		Set<TreeSet<BigInteger>> result = new TreeSet<TreeSet<BigInteger>>(new myComparator());

		// generate each subset one by one
		// I don't need the empty set and the full set, so start from 1 to N-1
		for (int i = 1; i < N-1; i++)
		{
			Set<BigInteger> set = new TreeSet<>();
		
			// check every bit of i
			for (int j = 0; j < N; j++)
			{
				// if j'th bit of i is set, add S.get(j) to current set
				if ((i & (1 << j)) != 0)
					set.add(key[j]);
			}
			if(set.size()>=minL) {
				result.add((TreeSet<BigInteger>) set);
			}
		}

		return result;
	}
	static class myComparator implements Comparator<TreeSet<BigInteger>>{
	    
	    @Override
	    public int compare(TreeSet<BigInteger> set1, TreeSet<BigInteger> set2) {
	        if(set1.size() > set2.size()){
	            return 1;
	        } else {
	            return -1;
	        }
	    }
	}
}
