package stream;

import java.util.PriorityQueue;
import java.util.Random;

import objects.MeshJoinObject;

import joins.HybridJoin;

/***
 * This program generates the benchmark that we used to test the performance of 
 * our HYBRIDJOIN. As mentioned in the paper our benchmark contains two characteristics.
 * (a) The rate of selling the product (80/20 Rule)
 * (b) The flow of selling transactions (Self-similar and bursty)
 * Our benchmark implements the Zipfian distribution which is one kind of Power's Law.
  */

public class CachejoinStartUpdatesStream extends Thread implements Comparable<Object>{
	public static TimeManagerCJ time;
	public boolean on=false;
	public double timeInChosenUnit;
	public DistributionClass distribution;
	public MyQueueCJ ownQueue;
	public double bandwidth;
	Random myRandom=new Random();
	
	public CachejoinStartUpdatesStream(){
		
	}
	public int compareTo(Object o) {
		CachejoinStartUpdatesStream y = (CachejoinStartUpdatesStream) o;
		   double diff = this.timeInChosenUnit - y.timeInChosenUnit;
		   if(diff < 0.0) return -1;
		   if(diff > 0.0) return 1;
		   return 0;
	 }
	public void run(){
		try{
			startStream();
		}catch (InterruptedException ie){
	        System.out.println(ie.getMessage());
	     }
	}
	
	CachejoinStartUpdatesStream(MyQueueCJ ownQueue, DistributionClass distribution, double bandwidth){
		this.distribution=distribution;
		this.ownQueue=ownQueue;
		this.bandwidth=bandwidth;
		timeInChosenUnit=System.nanoTime();
		swapStatus();
	}
	
	public void swapStatus(){
		
		timeInChosenUnit+=distribution.getNextDistributionValue()*TimeManagerCJ.STEP*bandwidth;
		
		if(on){
			ownQueue.totalCurrentBandwidth-=bandwidth;
			on=false;
		}
		             
		else{
			ownQueue.totalCurrentBandwidth+=bandwidth;
			on=true;
		}
		ownQueue.offer(this);	
	}
	public static void startStream()throws InterruptedException{
		
		DistributionClass distribution=new DistributionClass();
		DistributionClass generator=new DistributionClass();
		TimeManagerCJ time=new TimeManagerCJ();
		MyQueueCJ myQueue=new MyQueueCJ();
		int tuple=0,tupleValue=0;
		int count=0;
		long CS_per_Iteration=0,start=0,stop=0;
		for(int i=0; i<6; i++){
			new CachejoinStartUpdatesStream(myQueue,distribution,Math.pow(2,i));
		}
		CachejoinStartUpdatesStream current=(CachejoinStartUpdatesStream)myQueue.poll();
		while(true){
			tuple=0;
			time.waitOneStep();
			while(time.now()>current.timeInChosenUnit){
				current=(CachejoinStartUpdatesStream)myQueue.poll();
				current.swapStatus();
			}
			while(tuple<myQueue.totalCurrentBandwidth){
				tupleValue=generator.getNextDistributionValue();
				if(tupleValue>=1&& tupleValue<HybridJoin.DISK_RELATION_SIZE){
					start=System.nanoTime();
					HybridJoin.streamBuffer.put(new MeshJoinObject(tupleValue,tupleValue,tupleValue,tupleValue,tupleValue,System.currentTimeMillis()));
					stop=System.nanoTime();
					CS_per_Iteration+=stop-start;
					count++;
					if(count==HybridJoin.STREAM_SIZE){
						HybridJoin.CS[HybridJoin.CS_index++]=CS_per_Iteration/count;
						CS_per_Iteration=0;
						count=0;
					}
					tuple++;
					}
			}
		}
	}
}


class TimeManagerCJ{
	public final static int STEP=15;
	public double now(){
		return(System.nanoTime());
	}
	public void waitOneStep(){
		try{
			Thread.sleep(STEP);
		}catch (InterruptedException ie){
	        System.out.println(ie.getMessage());
	     }
	}
}

class MyQueueCJ extends PriorityQueue<CachejoinStartUpdatesStream>{
	private static final long serialVersionUID = 1L;
	public long totalCurrentBandwidth=0;
}
