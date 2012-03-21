package joins;

/**
 * This class implements the queue required in HYBRIDJOIN to store the record of
 * join attribute in stream tuples. Each node in the queue based on double link list,
 * containing the attribute value and the addresses of one step neighbour nodes. 
 * This class implements the all kind of operations related to queue and invoked from
 * HYBRIDJOIN programme.
 * @author asif
 *
 */

public class DoubleLinkQueue {
	private int item;
	private DoubleLinkQueue precede;
	private DoubleLinkQueue next;
	
	public DoubleLinkQueue(){
		this.precede=null;
		this.next=null;
	}
	
	public DoubleLinkQueue(int item) {
		this.item = item;
		this.precede = null;
		this.next = null;
	}
	
	public Object getItem() {
		return item;
	}
	
	public DoubleLinkQueue getPrecede() {
		return this.precede;
	}
	
	public DoubleLinkQueue getNext() {
			return this.next;
	}
	
	public void setItem(int item) {
		this.item = item;		
	}
	
	public void setPrecede(DoubleLinkQueue precede) {
		this.precede = precede;
	}
	
	public void setNext(DoubleLinkQueue next) {
		this.next = next;	
	}

	public DoubleLinkQueue addNode(int value){
		DoubleLinkQueue nextNode=new DoubleLinkQueue(value);
		this.setNext(nextNode);
		nextNode.setPrecede(this);
		nextNode.setNext(null);
		return nextNode;
	}
	
	public void deleteNode(boolean firstNode, boolean lastNode){
		if(firstNode){
			this.next.precede=null;
		}
		else if(lastNode){
			this.precede.next=null;
		}
		else{
			this.precede.next=this.next;
			this.next.precede=this.precede;
		}
	}
	
	public int popNode(){
		return this.next.item;
	}
	public void displayList(){
		DoubleLinkQueue start=this.getNext();
		while(start!=null){
			System.out.println(start.getItem());
			start=start.getNext();
		}
	}
	public int countNodes(){
		DoubleLinkQueue currentNode=this;
		int count=0;
		while(currentNode.getNext()!=null){
			currentNode=currentNode.getNext();
			count++;
		}
		return count;
	}
}
