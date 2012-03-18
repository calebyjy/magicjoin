package PartitionedJoin;

import java.util.Random;

public class PlainStream extends Thread {
	
	public static long t=0;
	public static long tn=0;
	public static double alpha=0.3;
	
	public void run(){
		try{
			startStream();
		}catch (InterruptedException ie){
	        System.out.println(ie.getMessage());
	     }
	}

	public static void startStream()throws InterruptedException{
		
		int tupleValue=0;int n=0;
		int e=0;
		Random myRandom=new Random();

		while(true){			
						
			int eTuple[];
				eTuple=new int[RangeBasedPartitionedJoin.P_NUMBER];

				if(RangeBasedPartitionedJoin.inBuffer.remainingCapacity()<10){
					Thread.sleep(1000);
				}
				
				while (RangeBasedPartitionedJoin.inBuffer.remainingCapacity()>0) {
					tn=System.currentTimeMillis();
					while (System.currentTimeMillis()-tn<1000){	
						tupleValue=myRandom.nextInt(20000000);
						int p=tupleValue%RangeBasedPartitionedJoin.P_NUMBER;
						if (tupleValue >= 1
								&& tupleValue < RangeBasedPartitionedJoin.R_SIZE) {
							RangeBasedPartitionedJoin.inBuffer
									.put(new PartitionedObject(tupleValue,
											tupleValue, tupleValue, tupleValue,
											tupleValue, System.currentTimeMillis()));
							
							System.out.println("put " + tupleValue+ " into inBuffer (" + n +")");
							System.out.println("inBuffer Size="+ RangeBasedPartitionedJoin.inBuffer.size());
							n++;

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
