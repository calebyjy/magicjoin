package PartitionedJoin;
import java.util.PriorityQueue;
import java.util.Random;
import java.lang.Math.*;
/***
 * This program generates the benchmark that we used to test the performance of 
 * our HYBRIDJOIN. As mentioned in the paper our benchmark contains two characteristics.
 * (a) The rate of selling the product (80/20 Rule)
 * (b) The flow of selling transactions (Self-similar and bursty)
 * Our benchmark implements the Zipfian distribution which is one kind of Power's Law.
  */

public class PStartUpdatesStream extends Thread implements Comparable<Object>{
	public static TimeManager2 time;
	public boolean on=false;
	public double timeInChosenUnit;
	public DistributionClass distribution;
	public MyQueue2 ownQueue;
	public double bandwidth;
	Random myRandom=new Random();
	
	public static long t=0;
	public static long tn=0;
	public static double alpha=0.3;
	
	
	PStartUpdatesStream(){
		
	}
	public int compareTo(Object o) {
		PStartUpdatesStream y = (PStartUpdatesStream) o;
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
	
	public PStartUpdatesStream(MyQueue2 ownQueue, DistributionClass distribution, double bandwidth){
		this.distribution=distribution;
		this.ownQueue=ownQueue;
		this.bandwidth=bandwidth;
		timeInChosenUnit=System.nanoTime();
		swapStatus();
	}
	
	public void swapStatus(){
		
		timeInChosenUnit+=distribution.getNextDistributionValue()*TimeManager2.STEP*bandwidth;
		
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
		TimeManager2 time=new TimeManager2();
		MyQueue2 myQueue=new MyQueue2();
		int tuple=0,tupleValue=0;int n=0;
//		int count=0;
		int e=0;
		//comment for a moment
		//long CS_per_Iteration=0,start=0,stop=0;
		for(int i=0; i<6; i++){
			new PStartUpdatesStream(myQueue,distribution,Math.pow(2,i));
		}
		PStartUpdatesStream current=(PStartUpdatesStream)myQueue.poll();
		while(true){			
						
			int eTuple[];
			tuple=0;
			//each epoch length is 10 seconds
				eTuple=new int[RangeBasedPartitionedJoin.P_NUMBER];
//				time.waitOneStep();
				while (time.now() > current.timeInChosenUnit) {
					current = (PStartUpdatesStream) myQueue.poll();
					current.swapStatus();
				}
				while (tuple < myQueue.totalCurrentBandwidth) {
					tn=System.currentTimeMillis();
					while (System.currentTimeMillis()-tn<1000){	
						tupleValue = generator.getNextDistributionValue();
						int p=tupleValue%RangeBasedPartitionedJoin.P_NUMBER;
						if (tupleValue >= 1
								&& tupleValue < RangeBasedPartitionedJoin.R_SIZE) {
	//						long start = System.nanoTime();
							RangeBasedPartitionedJoin.inBuffer
									.put(new PartitionedObject(tupleValue,
											tupleValue, tupleValue, tupleValue,
											tupleValue, System.currentTimeMillis()));
							
							System.out.println("put " + tupleValue+ " into inBuffer (" + n +")");
							n++;
							
	/*						long stop = System.nanoTime();
							long CS_per_Iteration = stop - start;
							count++;
							if (count == RangeBasedPartitionedJoin.WAIT_BUFFER) {
								comment for a moment
								RangeBasedPartitionedJoin.CS[RangeBasedPartitionedJoin.CS_index++]=CS_per_Iteration/count;
								CS_per_Iteration = 0;
								count = 0;
							}*/
							tuple++;
							eTuple[p]++; 
						}
					}	
					e++;
					for (int i=0;i<RangeBasedPartitionedJoin.P_NUMBER;i++){
						if(eTuple[i]!=0){
							RangeBasedPartitionedJoin.C[i]=RangeBasedPartitionedJoin.C[i]+eTuple[i]*Math.pow(alpha,e);
						}
					}	
				}
		}
	}
}


class TimeManager2{
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

class MyQueue2 extends PriorityQueue<PStartUpdatesStream>{
	private static final long serialVersionUID = 1L;
	public long totalCurrentBandwidth=0;
}
