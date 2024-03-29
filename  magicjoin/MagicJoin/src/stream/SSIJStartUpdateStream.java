package stream;

import java.util.PriorityQueue;
import java.util.Random;

import objects.SSIJObject;
import joins.SSIJ;

/***
 * This program generates the benchmark that we used to test the performance of 
 * our SSIJ. As mentioned in the paper our benchmark contains two characteristics.
 * (a) The rate of selling the product (80/20 Rule)
 * (b) The flow of selling transactions (Self-similar and bursty)
 * Our benchmark implements the Zipfian distribution which is one kind of Power's Law.
  */

public class SSIJStartUpdateStream extends Thread implements Comparable<Object>{
	public static TimeManagerSSIJ time;
	public boolean on=false;
	public double timeInChosenUnit;
	public DistributionClass distribution;
	public MyQueueSSIJ ownQueue;
	public double bandwidth;
	Random myRandom=new Random();
	
	public SSIJStartUpdateStream(){
		
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
	
	SSIJStartUpdateStream(MyQueueSSIJ ownQueue, DistributionClass distribution, double bandwidth){
		this.distribution=distribution;
		this.ownQueue=ownQueue;
		this.bandwidth=bandwidth;
		timeInChosenUnit=System.nanoTime();
		swapStatus();
	}
	
	public void swapStatus(){
		
		timeInChosenUnit+=distribution.getNextDistributionValue()*TimeManagerSSIJ.STEP*bandwidth;
		
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
		TimeManagerSSIJ time=new TimeManagerSSIJ();
		MyQueueSSIJ myQueue=new MyQueueSSIJ();
		int tuple=0,tupleValue=0;
		int count=0;
		long CS_per_Iteration=0,start=0,stop=0;
		for(int i=0; i<6; i++){
			new SSIJStartUpdateStream(myQueue,distribution,Math.pow(2,i));
		}
		SSIJStartUpdateStream current=(SSIJStartUpdateStream)myQueue.poll();
		while(true){
			tuple=0;
			time.waitOneStep();
			while(time.now()>current.timeInChosenUnit){
				current=(SSIJStartUpdateStream)myQueue.poll();
				current.swapStatus();
			}
			while(tuple<myQueue.totalCurrentBandwidth){
				tupleValue=generator.getNextDistributionValue();
				if(tupleValue>=1&& tupleValue<SSIJ.DISK_RELATION_SIZE){
					start=System.nanoTime();
					SSIJ.sBuffer.put(new SSIJObject(tupleValue,tupleValue,tupleValue,tupleValue,tupleValue,System.nanoTime()));
					stop=System.nanoTime();
					CS_per_Iteration+=stop-start;
					count++;
					if(count==SSIJ.STREAM_SIZE){
						SSIJ.CS[SSIJ.CS_index++]=CS_per_Iteration/count;
						CS_per_Iteration=0;
						count=0;
					}
					tuple++;
				}
			}
		}
	}
}


class TimeManagerSSIJ{
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

class MyQueueSSIJ extends PriorityQueue<SSIJStartUpdateStream>{
	private static final long serialVersionUID = 1L;
	public long totalCurrentBandwidth=0;
}
