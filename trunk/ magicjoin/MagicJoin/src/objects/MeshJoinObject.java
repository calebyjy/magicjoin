package objects;

/**
 * Since the tuples stored in multi-hash-map in the for of objects therefore we 
 * need to create the object for each tuple of stream S. This class is simply do this.
 * @author asif
 *
 */
public class MeshJoinObject {
	public int attr1,attr2,attr3,attr4,attr5;
	public long arrivalTime=0;

	public MeshJoinObject(int attr1,int attr2, int attr3, int attr4, int attr5,long arrivalTime){
		this.attr1=attr1;
		this.attr2=attr2;
		this.attr3=attr3;
		this.attr4=attr4;
		this.attr5=attr5;
		this.arrivalTime=arrivalTime;
				
	}
}
