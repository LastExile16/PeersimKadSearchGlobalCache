package peersim;

import java.io.FileWriter;
import java.io.IOException;
import java.util.NavigableMap;
import java.util.TreeMap;

import com.opencsv.CSVWriter;

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
    	E selectedKeyword = map.higherEntry(value).getValue();
    	
    	/*String csv = "findValueGeneratedQueries.csv";
        CSVWriter writer = null;
		try {
			writer = new CSVWriter(new FileWriter(csv, true));
		} catch (IOException e) {
			e.printStackTrace();
		}
          
        //Create record
        String [] record = {selectedKeyword.toString()};
        //Write the record to file
        writer.writeNext(record);
          
        //close the writer
        try {
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
    	
        return selectedKeyword;
    }
    
    public E testDistribution() {
    	String csv = "findValueGeneratedQueries.csv";
        CSVWriter writer = null;
		try {
			writer = new CSVWriter(new FileWriter(csv, true));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
		E selectedKeyword = null;
		double value = 0.0;
		int no_of_issued_queries_in_dataset = 1500000;
		for(int i=0; i<no_of_issued_queries_in_dataset; i++) {
			// get a random key
			
			
    	// double value = random.nextDouble() * total;
        value = CommonState.r.nextDouble() * total;
    	selectedKeyword = map.higherEntry(value).getValue();
	    	if(i%10000 == 0) 
				System.out.println(selectedKeyword.toString());
			
    	
          
        //Create record
        // String [] record = {selectedKeyword.toString()};
        //Write the record to file
        writer.writeNext(new String[]{selectedKeyword.toString()});
    	}
        //close the writer
        try {
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	System.exit(1);
        return selectedKeyword;
    }
}