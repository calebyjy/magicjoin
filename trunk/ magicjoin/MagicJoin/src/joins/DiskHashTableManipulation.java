package joins;

import java.util.Hashtable; 

import objects.HybridJoinDiskObject;
/***
 *author asif
 *This class implements the stream probing module of CACHEJOIN. The class contains a method with
 *name matchedIntoDiskHash. The main purpose of this method is to check whether the stream tuple
 *is matched in disk-based hash HR or not. If the stream tuple matched this method returns true 
 *otherwise false.     
 */

public class DiskHashTableManipulation{
	public static final int NON_SWAP_DB=2500;
	public static final int START=1;
	static Hashtable<Integer,HybridJoinDiskObject> dmhm=new Hashtable<Integer,HybridJoinDiskObject>();
	HybridJoinDiskObject hashValue;
	DiskHashTableManipulation(){

	}

	public boolean matchedIntoDiskHash(int key){

		if(dmhm.containsKey(key)){
			hashValue=(HybridJoinDiskObject)dmhm.get(key);
			return true;
		}
		else
			return false;
	}
}
