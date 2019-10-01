package peersim;

import java.io.IOException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import joinery.DataFrame;

/**
 * It was SHA1 generator, but I will change the parameter to be SHA256 generator 
 * @author chinese guy github
 * @author Nawras Nazar
 *
 */
public  class SHA1 {
	/**
	 * Encode the given string with SHA-1
	 * @param  the string to be encoded 
	 * @return the encoded string
	 * @throws Exception
	 */
    public static String shaEncode(String inStr) throws Exception {
        MessageDigest sha = null;
        try {
            sha = MessageDigest.getInstance("SHA-1");
        } catch (Exception e) {
            System.out.println(e.toString());
            e.printStackTrace();
            return "";
        }
        byte[] byteArray = inStr.getBytes("UTF-8");
        byte[] md5Bytes = sha.digest(byteArray);
        StringBuffer hexValue = new StringBuffer();
        for (int i = 0; i < md5Bytes.length; i++) {
            int val = ((int) md5Bytes[i]) & 0xff;
            if (val < 16) {
                hexValue.append("0");
            }
            hexValue.append(Integer.toHexString(val));
        }
        return hexValue.toString();
    }

    public static void main(String args[]) throws Exception {
    	//1- two generated queries are combined
    	//2- encoded by SHA1
    	//3- stored as BigInteger in base16
    	//4- print out the result in base10; that is the new query.
		String sss = "441360378140870817611248411035166340804225760090"+"1261425585535595253771221081958815840050577628176";
		 System.out.println("SHA SSS：" + shaEncode(sss));
		 BigInteger bint = new BigInteger(shaEncode(sss),16);
	        System.out.println(bint.toString());
	        
        String str = UUID.randomUUID().toString();
        System.out.println("Random Plain：" + str);
        System.out.println("SHA Func：" + shaEncode(str));
        BigInteger bigInteger = new BigInteger(shaEncode(str),16);
        System.out.println(bigInteger);
        BigInteger bn3 = new BigInteger("2");
        BigInteger bn4 = new BigInteger("1");

        for (int i= 0; i < 160; i++) {
            bn4=bn4.multiply(bn3);
        }
        System.out.println(bn4);
        System.out.println(bigInteger.compareTo(bn4));
        /////////////////////////////////////////
        
        String value = UUID.randomUUID().toString().replace("-","");
        System.out.println("Value: " + value);
		BigInteger key = null;
		try {
			key = new BigInteger(SHA1.shaEncode(value), 16);
			System.out.println("key: "+ key.toString()); // base 10
			System.out.println("key: "+ key.toString(16)); // base 16
		} catch (Exception e) {
			e.printStackTrace();
		}
		DataFrame df = null;
		try {
			df = DataFrame.readCsv("frequency-search-logs-with-header.csv");
		} catch (IOException e1) {
			System.out.print("dataset not accesible");
			e1.printStackTrace();
		}
		
		// nextInt is normally exclusive of the top value [min, max)
		int rand = ThreadLocalRandom.current().nextInt(0, (int) df.count().col(0).get(0));
		String k = (String) df.col(0).get(450);
		Set<String> v = new HashSet<String>(Arrays.asList(((String) df.col(1).get(450)).split(", ")));
		System.out.println(rand);
		System.out.println(k);
		System.out.println(v);
		// df.plot();
		
    }
}
