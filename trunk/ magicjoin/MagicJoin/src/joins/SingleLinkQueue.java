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

public class SingleLinkQueue {
	private int item;
	private SingleLinkQueue next;
	
	public SingleLinkQueue(){
		this.next=null;
	}
	
	public SingleLinkQueue(int item) {
		this.item = item;
		this.next = null;
	}
	
	public Object getItem() {
		return item;
	}
	
	public SingleLinkQueue getNext() {
			return this.next;
	}
	
	public void setItem(int item) {
		this.item = item;		
	}

	}
	
	public void setNext(SingleLinkQueue next) {
		this.next = next;	
	}

	public SingleLinkQueue addNode(int value){
		SingleLinkQueue nextNode=new SingleLinkQueue(value);
		this.setNext(nextNode);
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
		SingleLinkQueue start=this.getNext();
		while(start!=null){
			System.out.println(start.getItem());
			start=start.getNext();
		}
	}
	public int countNodes(){
		SingleLinkQueue currentNode=this;
		int count=0;
		while(currentNode.getNext()!=null){
			currentNode=currentNode.getNext();
			count++;
		}
		return count;
	}
}

