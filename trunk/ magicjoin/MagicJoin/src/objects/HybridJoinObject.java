package objects;
/**
 * This class only convert the stream attributes values in the form of HYBRIDJOIN
 * Object before storing into hash table
 * @author asif
 *
 */

public class HybridJoinObject {
			public int attr1,attr2,attr3,attr4,attr5;
			public long arrivalTime=0;
			Queue nodeAddress;

		HybridJoinObject(int attr1,int attr2, int attr3, int attr4, int attr5,Queue nodeAddress, long arrivalTime){
			this.attr1=attr1;
			this.attr2=attr2;
			this.attr3=attr3;
			this.attr4=attr4;
			this.attr5=attr5;
			this.nodeAddress=nodeAddress;
			this.arrivalTime=arrivalTime;
								
		}
	}

