 
package PartitionedJoin;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * @author gxia003
 *
 */
public class RangeBasedPartitionedJoin extends Thread{
	//Disk-relation
	public static final int R_SIZE=2000000;
	public static final int DISK_BUFFER=500;
	public static final int PAGE_SIZE=500;
	public static final int P_NUMBER=R_SIZE/PAGE_SIZE;
	//this index records the number of tuples in each wait_buffer partition
	//public static int[] index=new int[P_NUMBER];
	//the index records the workloads of each wait buffer partitions
	public static int[] pLoad=new int[P_NUMBER];
	
	//Stream-buffer
	public static final int WAIT_BUFFER=2000;
	//public static final int JOIN_MEMORY=100000;
	public static final int JOIN_BUFFER_SIZE=500;//in number partitions
	//basic partition size (in tuple)
	public static final int BASIC_PARTITION_SIZE=16;
	//the size of each block in wait buffer partitions
	public static final int BLOCK_SIZE=16;	
	//input buffer size
	public static final int INPUT_BUFFER=1000;
	//max and minimum boundary to switch block and tuple mode
	public static final int L_MARK=100;
	//capacity of wait buffer
	public static final int CAP=WAIT_BUFFER  ;
	//diskPorbe invocation threshold
    int nIvk=3000;
	//user defined fraction rwb
	public static final double RWB=0.4;	
	//user defiend parameter rho
	public static final double RHO=0;
	private static final String table = "rpjoin";
	//total size of wait buffer
	static int twSize=0;
	//size of free space for wait buffer
	static int fwSize=100000;
	
	//arrival frequency of each partition	
	public static double C[]=new double[P_NUMBER];
	
	static int dBuffer[][]=new int[DISK_BUFFER][30];
	static int jMemory[][][]=new int[JOIN_BUFFER_SIZE][PAGE_SIZE][30];
	//jMemoryInfo records the partitions which are currently in the jMemory and their head location
	static int[]jMemoryInfo=new int[JOIN_BUFFER_SIZE];
	//	static PartitionedObject wBuffer[][]=new PartitionedObject[P_NUMBER][];//!!!the array size here need be specified later
	
	static ArrayList<LinkedList<PartitionedObject>> wBuffer=new ArrayList<LinkedList<PartitionedObject>>(P_NUMBER);
	static ArrayBlockingQueue<PartitionedObject> inBuffer = new ArrayBlockingQueue<PartitionedObject>(INPUT_BUFFER);
	
	Connection conn=null;
	Statement stmt;
	ResultSet rs=null;

	static long[]CS=new long[P_NUMBER];
	
	//testing var
	int test=0;
	static int[] etuple=new int[P_NUMBER];
	long processed=0;
	int index=0;
	long[] waittime=new long[1000];
	String[] mode=new String[1000];
	static BufferedWriter bw;
	int round=0;

	
public static void main(String arg[])throws java.io.IOException, InterruptedException{
		
		for(int i=0;i<JOIN_BUFFER_SIZE;i++){
			for(int j=0;j<PAGE_SIZE;j++){
				for(int k=0;k<30;k++){
					jMemory[i][j][k]=-1;
				}
			}
		}
		
		for(int i=0;i<JOIN_BUFFER_SIZE;i++){
			jMemoryInfo[i]=-1;
		}
		
		for (int i=0; i<P_NUMBER; i++){
			LinkedList<PartitionedObject> p=new LinkedList<PartitionedObject>();
			wBuffer.add(p);
		}
		
		bw=new BufferedWriter(new FileWriter("e:\\workspace\\result\\test.txt"));
		bw.write("test result"+"\t\t");

		
		RangeBasedPartitionedJoin rpj=new RangeBasedPartitionedJoin();
		PStartUpdatesStream stream=new PStartUpdatesStream();
		//PlainStream stream=new PlainStream();
		System.out.println("Ranged-based partition Join in execution mode...");
		stream.start();
		rpj.start();

/*		RangeBasedPartitionedJoin rpj=new RangeBasedPartitionedJoin();
		PStartUpdatesStream stream=new PStartUpdatesStream();
		stream.start();
		
		while(true){
			if(inBuffer.isEmpty()==false){
				System.out.println(inBuffer.poll().attr1);
			}
			
		}*/
	}
	
		
	public Connection connectDB(){
		Connection conn=null;
		Properties connectionProps=new Properties();
		connectionProps.put("user", "root");
		connectionProps.put("password", "sunshine");
		try {	
			conn=DriverManager.getConnection("jdbc:mysql://localhost/testdata",connectionProps);
			System.out.println("Connection established");
		} catch (Exception e) {
			System.err.println("problem on connection stage");
		}
		return conn;
	}
	
	public void closeConnection(Connection con){
		try{
			if(con!=null){
				con.close();
				System.out.println("Database connection closed");
			}
		}catch (SQLException e)
		{
			System.err.println (e);
		}
	}
	
	//the method which distinguish tuple or block mode of the algorithm, and also load 
	//data from input stream into the wait-buffer at the beginning the program runs
/*	public void fillWaitBuffer(){
		int tuples=0;
		if (inBuffer.size()>L_MARK) {
			while (tuples < WAIT_BUFFER) {
				int id = inBuffer.peek().attr1;
				int p = id / PAGE_SIZE;
				wBuffer.get(p).add(inBuffer.poll()) ;
				index[p]++;
			}
		}
		else{
			tupleMode(inBuffer.poll());
		}
		
	}*/
	
	//replace a partition in the join memory
	//npNmuber: the partition id of success partition
	//opNumber: the id of the partition will be replaced
	public void replaceJoinBuffer(int npNumber, int opNumber){
		boolean match=false;
		int startRead=npNumber*PAGE_SIZE;
		int startReplace=0;//the location in jMemoryInfo & jMemory

		for (int i=0;i<jMemoryInfo.length;i++){
			if (jMemoryInfo[i]==opNumber) {
				startReplace=i;
				match=true;
				break;
			}		
		}			
		if (match==false) {
			System.out.println("RP: partition is not existed in jMemory!");
			return;
		}
		try{
			rs=stmt.executeQuery("SELECT * from " +table+ " where attr1>="+startRead+" AND attr1<"+(startRead+DISK_BUFFER)+"");
			int n=0;	
			while(rs.next()){									
				for(int col=1; col<=30; col++){
					jMemory[startReplace][n][col-1]=rs.getInt(col);
				}
				n++;
			}
			jMemoryInfo[startReplace]=npNumber;
		}catch(SQLException e){System.out.print(e);}		 

	}
	
	
	
	//tuple mode
	private void tupleMode(PartitionedObject po) {
		boolean match=false;
		inBuffer.poll(); //delete the tuple from the stream buffer
 		for(int i=0; i<PAGE_SIZE;i++){
			if(dBuffer[i][0]==po.attr1){				
				match=true;
				//join operation here
				System.out.println("TM: Join complete: "+ po.attr1);
				processed++;
				waittime[index]=System.currentTimeMillis()-po.arrivalTime;
				mode[index++]="TM";
				if(index>=99){				
					try {
						bw.write("the "+ ++round +"th 100 processed tuples:"+ "\t\t");
						for(int idx=0; idx<100;idx++){
							bw.write(mode[idx]+": process time:"+ waittime[idx]+ "\t\t");
						}
					} catch (IOException e) {
						e.printStackTrace();
					}				
					index=0;
				}
				break;
			}
		}		
		if(match==false){
			int p=po.attr1/PAGE_SIZE;
			wBuffer.get(p). add(po);
			fwSize--;
			twSize++;
		}
		
		if(twSize>=CAP){
			int max=-1;			
			while(fwSize<RWB*CAP){
				int size=0;
				for(int i=0; i<P_NUMBER;i++){
					if (wBuffer.get(i).size()>size) {
						size=wBuffer.get(i).size();
						max=i;
					}
				}

				diskProbe(max);

				System.out.println("TM-DP: twsizex= "+ twSize);
				System.out.println("TM-DP: fwsizex= "+ fwSize);
				 System.out.println("TM-DP: processed:"+ processed);
			}			
		}
	}
	
	//block mode
	/**
	 * @param po
	 */
	private void blockMode(PartitionedObject po){
		boolean match=false;
		int jl=0; 
		int p = po.attr1 / PAGE_SIZE;

		wBuffer.get(p). add(po);
		twSize++;
		fwSize--;
		
		if(wBuffer.get(p).size()>nIvk){
			diskProbe(p);
			return;
		}
		
		//check if the partition of incoming stream is in the join memory, and if it does, then get its location in the join memory
		for (int i=0;i<JOIN_BUFFER_SIZE;i++){
			if (jMemoryInfo[i]==p) {
				jl=i;
				System.out.println("jl= "+i);
				System.out.println("p= "+p);
				match=true; 
				break;
			}		
		}

		if((wBuffer.get(p).size()%BLOCK_SIZE)==0 && match==true){
			for(int i = wBuffer.get(p).size()-1; i > wBuffer.get(p).size()-BLOCK_SIZE-1&&i>=0	; i--){

				for (int j = 0; j < PAGE_SIZE; j++) {
					if (wBuffer.get(p).get(i).attr1==jMemory[jl][j][0]){
						
						System.out.println("BM: Join complete on: "+ wBuffer.get(p).get(i).attr1);
						processed++;
						waittime[index]=System.currentTimeMillis()-wBuffer.get(p).get(i).arrivalTime;
						mode[index++]="BM";
						if(index>=99){				
							try {
								bw.write("the "+ ++round +"th 100 processed tuples:"+ "\t\t");
								for(int idx=0; idx<100;idx++){
									bw.write(mode[idx]+ ": process time:"+ waittime[idx]+ "\t\t");
								}
							} catch (IOException e) {
								e.printStackTrace();
							}				
							index=0;
						}
						wBuffer.get(p).remove(i);
						twSize--;
						fwSize++;
						break;
						//join operation here
					}
				} 
			}
		}else if ((wBuffer.get(p).size()%BLOCK_SIZE)==0 && match==false) {
			System.out.println("BM: partition is not existed in jMemory!");
			//wBuffer.get(p).add(po);
			//join operation here
		}else if((wBuffer.get(p).size()%BLOCK_SIZE)!=0){
			//wBuffer.get(p).add(po);
		}
		
		System.out.println("TEST1");
		System.out.println(twSize);

		if(twSize>=CAP){
			int max=-1;	
			
			while(fwSize<RWB*CAP){
				int size=0;
				for(int i=0; i<P_NUMBER;i++){
					if (wBuffer.get(i).size()>size) {
						size=wBuffer.get(i).size();
						max=i;
					}
				}
/*				int btwsize=0;
				for (int i=0; i<P_NUMBER; i++){
					btwsize+=wBuffer.get(i).size();
				}*/
				diskProbe(max);
/*
				for (int i=0; i<P_NUMBER; i++){
					System.out.println("C "+i+" ="+ C[i]);
				}
				for (int i=0; i<P_NUMBER;i++){
					System.out.println("wBuffer "+i+ " size: " +wBuffer.get(i).size());
				}
				int tws=0;	
				for (int i=0; i<P_NUMBER; i++){
					tws+=wBuffer.get(i).size();
				}

				System.out.println("btwsizex= "+ btwsize);
				System.out.println("tfwsize= "+fwSize);
				System.out.println("twsizex= "+ twSize);
				System.out.println("tws= "+ tws+ " max= "+ max);
				
				for(int i=0; i<JOIN_BUFFER_SIZE; i++){
					System.out.println(jMemoryInfo[i]+ " is in jMemory");
				}
				
				
				for(int i=0; i<waittime.length;i++){
					System.out.println(waittime[i]);
				}
				System.exit(0);*/
				System.out.println("twsizex= "+ twSize);
				System.out.println("fwsizex= "+ fwSize);
				 System.out.println("processed:"+ processed);
			}			
		}
	}

	private void diskProbe(int p) {
		int startRead=p*PAGE_SIZE;

		try{
			rs=stmt.executeQuery("SELECT * from "+table+" where attr1>="+startRead+" AND attr1<"+(startRead+DISK_BUFFER));
			int n=0;	
			while(rs.next()){									
				for(int col=1; col<=30; col++){
					dBuffer[n][col-1]=rs.getInt(col);		
					
				}System.out.println("put "+ dBuffer[n][0]+" into dBuffer!");
				n++;					
			}
		}catch(SQLException e){System.out.print(e);}
		
		for (int i=0; i<PAGE_SIZE; i++){
			for (int j=0; j<wBuffer.get(p).size();j++){
				if(wBuffer.get(p).get(j).attr1==dBuffer[i][0]){
					System.out.println("DP: Join complete "+wBuffer.get(p).get(j).attr1 + " on: "+ dBuffer[i][0]);
					processed++;
					waittime[index]=System.currentTimeMillis()-wBuffer.get(p).get(j).arrivalTime;
					mode[index++]="DP";
					if(index>=99){				
						try {
							bw.write("the "+ ++round +"th 100 processed tuples:"+ "\t\t");
							for(int idx=0; idx<100;idx++){
								bw.write(mode[idx]+ ": process time:"+ waittime[idx]+ "\t\t");
							}
						} catch (IOException e) {
							e.printStackTrace();
						}				
						index=0;
					}
					wBuffer.get(p).remove(j);
					twSize--;
					fwSize++;
					//join operation
				}
			}
		}
		
		for (int i=0; i<P_NUMBER; i++){
			//we presume that all partitions are in the same size, so we use "1" to replace |Pi|/|Pm|, and ignore Mfree+|P|>=|Pi| precondition
			//the frequency C[i] is computed in the stream generator--the startStream method of PStartUpdatesStream class
			if(C[p]>=1* (1+RHO)*C[i]) {
				replaceJoinBuffer(p, i);
				break;
			}
		}		
	}
	
	public boolean initMode(PartitionedObject po){
		int p = po.attr1 / PAGE_SIZE;
		int startRead=p*PAGE_SIZE;
		int l=0;
		int ml=0;
		boolean capable=false;
		boolean exist=false;
		
		
		for(int i=0; i<JOIN_BUFFER_SIZE;i++){
			if(jMemoryInfo[i]==p){
				exist=true;
				ml=i;
				break;
			}			
		}
		
		//if the partition is already in the join buffer, join and return
		if(exist==true){
			for(int i=0; i<PAGE_SIZE; i++){
				if(po.attr1==jMemory[ml][i][0]){
					System.out.println("INIT(exist): Join complete: "+ po.attr1 +" " +jMemory[ml][i][0]);
					processed++;
					waittime[index]=System.currentTimeMillis()-po.arrivalTime;
					mode[index++]="IM";
					if(index>=99){				
						try {
							bw.write("the "+ ++round +"th 100 processed tuples:"+ "\t\t");
							for(int idx=0; idx<100;idx++){
								bw.write(mode[idx]+ ": process time:"+ waittime[idx]+ "\t\t");
							}
						} catch (IOException e) {

							e.printStackTrace();
						}				
						index=0;
					}
					return true;
				}
			}
		}
		
		//find the first place which is available in the join buffer
		for(int i=0; i<jMemory.length; i++){
			if (jMemory[i][0][0]==-1) {
				l=i;
				capable=true;
				break;
			}			
		}
		
		if(capable==false){
			System.out.println("INIT: join memory is full, can't insert");
			return capable;
		}
		
		try{
			rs=stmt.executeQuery("SELECT * from "+ table+ " where attr1>="+startRead+" AND attr1<"+(startRead+DISK_BUFFER)+"");
			int n=0;	
			while(rs.next()){									
				for(int col=1; col<=30; col++){
					jMemory[l][n][col-1]=rs.getInt(col);
				}
				if(po.attr1==rs.getInt(1)){
					System.out.println("INIT(firsttime): Join complete: "+ po.attr1 +" " +jMemory[l][n][0]);
					processed++;
					waittime[index]=System.currentTimeMillis()-po.arrivalTime;
					mode[index++]="IM";
					if(index>=99){				
						try {
							bw.write("the "+ ++round +"th 100 processed tuples:"+ "\t\t");
							for(int idx=0; idx<100;idx++){
								bw.write(mode[idx]+": process time:"+ waittime[idx]+ "\t\t");
							}
						} catch (IOException e) {
							e.printStackTrace();
						}				
						index=0;
					}
				}
				n++;
			}
			jMemoryInfo[l]=p;
		}catch(SQLException e){System.out.print(e);}	
		return capable;
	}

	
	public RangeBasedPartitionedJoin(){

	}
	
	public void startPJoin() throws InterruptedException{
		Connection con=connectDB();
		try {
			stmt=con.createStatement();
			stmt.setFetchSize(PAGE_SIZE);
		} catch (SQLException e1) {
			e1.printStackTrace();
		}
		
		boolean capable=true;
		while(capable){
			capable=initMode(inBuffer.poll());
		}
		
		while(true){
			
			if(inBuffer.isEmpty()&&fwSize<WAIT_BUFFER){
				int max=-1;
				int size=0;
				for(int i=0; i<P_NUMBER;i++){
					if (wBuffer.get(i).size()>size) {
						size=wBuffer.get(i).size();
						max=i;
					}
				}
				diskProbe(max);
			}
			else if (inBuffer.isEmpty()==false && inBuffer.size()<L_MARK){
				tupleMode(inBuffer.poll());
			}			
			else if (inBuffer.isEmpty()==false && inBuffer.size()>=L_MARK){
				blockMode(inBuffer.poll());
			}
			else if(inBuffer.isEmpty()==true&&fwSize==WAIT_BUFFER){
				synchronized (this) {
					try {
						wait(1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		}
	}
	
	public void run(){
		try{
			startPJoin();
		}catch (InterruptedException ie){
	        System.out.println(ie.getMessage());
	     }
	}
	
}
