package Objects;

public class DiskObject1 {
	/**
	 * @author Grant
	 *This class converts input stream tuples into Partitioned Join objects, 
	 *for storing them into the hash table.
	 */
	public static int PAGE_SIZE=10000;
	public int id,credit,partitionId;
	public String name,city;
	
	public DiskObject1(int id, String name, String city){
		this.id=id;
		this.name=name;
		this.city=city;
		this.partitionId=id%PAGE_SIZE;		
	}
		
}
