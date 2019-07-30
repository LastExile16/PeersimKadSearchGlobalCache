package peersim;

import java.util.NavigableMap;
import java.util.TreeMap;
import peersim.core.CommonState;

/**
 * Use {@link NavigableMap} to select a random item according to the overall distribution of all items, using this class we can give each item some weight then
 * draw randomly according to its weight  
 * @see from https://stackoverflow.com/questions/6409652/random-weighted-selection-in-java/6409767#6409767 with some changes
 * @author Nawras Nazar
 */


public class RandomCollection<E> {
    private final NavigableMap<Double, E> map = new TreeMap<Double, E>();
    // private final Random random;
    private double total = 0;

    public RandomCollection() {
        // this(new Random());
    }

    /*public RandomCollection(Random random) {
        this.random = random;
    }*/

    public RandomCollection<E> add(double weight, E result) {
        if (weight <= 0) return this;
        total += weight;
        map.put(total, result);
        return this;
    }

    public E next() {
    	// double value = random.nextDouble() * total;
        double value = CommonState.r.nextDouble() * total;
        return map.higherEntry(value).getValue();
    }
}