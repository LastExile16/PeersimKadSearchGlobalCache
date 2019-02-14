package peersim;

import java.math.BigInteger;

/**
 * 
 * @author chinese guy from github
 * @author Nawras
 * This class is used as store request object by specifing the item-key, size, value 
 *
 */
public class StoreFile {

    private BigInteger key;
    private Object value;
    
    /**
     * currently =64, but kv size should correspond the the size of the kv content!
     */
    private int size = 64;
    private int storeNodeRemainSize;
    
    /**
     * 
     * @param key
     * 			Hashed Key
     * @param value
     * 			Original Value
     */
    public StoreFile(BigInteger key, Object value) {
        this.key = key;
        this.value = value;
    }
    
    public Object clone() throws CloneNotSupportedException 
	{ 
    	return super.clone(); 
	} 
    @Override
    public String toString() {
        return "StoreFile{" +
                "key=" + key +
                ", value=" + value +
                ", size=" + size +
                ", storeNodeRemainSize=" + storeNodeRemainSize +
                '}';
    }

    public BigInteger getKey() {
        return key;
    }

    public void setKey(BigInteger key) {
        this.key = key;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }
    
    /**
     * size of the current StoreFile message
     * @return
     * 		int of file size
     */
    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public int getStoreNodeRemainSize() {
        return storeNodeRemainSize;
    }

    public void setStoreNodeRemainSize(int storeNodeRemainSize) {
        this.storeNodeRemainSize = storeNodeRemainSize;
    }
}
