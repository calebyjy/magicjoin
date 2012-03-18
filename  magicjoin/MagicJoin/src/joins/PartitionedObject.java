package joins;
/**
 * @author Grant
 *This class converts input stream tuples into Partitioned Join objects, 
 *for storing them into the hash table.
 */
public class PartitionedObject {
	public int attr1,attr2,attr3,attr4,attr5;
	public long arrivalTime=0;

	public PartitionedObject(int attr1,int attr2, int attr3, int attr4, int attr5, long arrivalTime){
	this.attr1=attr1;
	this.attr2=attr2;
	this.attr3=attr3;
	this.attr4=attr4;
	this.attr5=attr5;

	this.arrivalTime=arrivalTime;
	}
}
