package objects;

import joins.DoubleLinkQueue;
import joins.SingleLinkQueue;

public class SSIJObject {
	public int attr1,attr2,attr3,attr4,attr5;
	public long arrivalTime=0;
	public DoubleLinkQueue nodeAddress1;
	public SingleLinkQueue nodeAddress2;

	public HybridJoinObject(int attr1,int attr2, int attr3, int attr4, int attr5,DoubleLinkQueue nodeAddress, long arrivalTime){
		this.attr1=attr1;
		this.attr2=attr2;
		this.attr3=attr3;
		this.attr4=attr4;
		this.attr5=attr5;
		this.nodeAddress1=nodeAddress;
		this.arrivalTime=arrivalTime;
							
	}
	
	public HybridJoinObject(int attr1,int attr2, int attr3, int attr4, int attr5,SingleLinkQueue nodeAddress, long arrivalTime){
		this.attr1=attr1;
		this.attr2=attr2;
		this.attr3=attr3;
		this.attr4=attr4;
		this.attr5=attr5;
		this.nodeAddress2=nodeAddress;
		this.arrivalTime=arrivalTime;
						
}
}
