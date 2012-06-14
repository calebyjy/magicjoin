package joins;

import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.DriverManager;
import org.apache.commons.collections15.MultiMap;
import org.apache.commons.collections15.multimap.MultiHashMap;
import sizeof.agent.SizeOfAgent;
import objects.HybridJoinObject;
import objects.MeshJoinObject;
import stream.HybridjoinStartUpdatesStream;
/**
 * This program measures the waiting time and processing time in HYBRIDJOIN. In addition this also
 * calculates the processing cost for each loop iteration that is used to calculate the
 * service rate. 
 * @author asif
 *
 */
public class HybridJoin {
	public static final int HASH_SIZE=317597;
	public static final int QUEUE_SIZE=HASH_SIZE;
	public static final int STREAM_SIZE=50000;
	public static final int DISK_RELATION_SIZE=10000000;
	public static final int PAGE_SIZE=500;
	public static final int MIN_KEY=1;
	public static final int MAX_KEY=DISK_RELATION_SIZE;
	
	//***************MEASUREMENT LIMITS************
	public static final int MEASUREMENT_START=1;
	public static final int MEASUREMENT_STOP=DISK_RELATION_SIZE;
		 
	static MultiMap<Integer,HybridJoinObject> mhm=new MultiHashMap<Integer,HybridJoinObject>();
	static ArrayList <HybridJoinObject> list=new ArrayList<HybridJoinObject>();
	public static LinkedBlockingQueue<MeshJoinObject> streamBuffer=new LinkedBlockingQueue<MeshJoinObject>();
	static int diskBuffer[][]=new int[PAGE_SIZE][30];
	Random myRandom=new Random();
	Statement stmt=null;
	ResultSet rs=null;
	DoubleLinkQueue head,currentNode;
	CalculateAccessPageFrequencyOfR CAPFR=new CalculateAccessPageFrequencyOfR();
	int streamRandomValue;
	int requiredTuplesCount=0;
	static long CE[]= new long[DISK_RELATION_SIZE/PAGE_SIZE];
	public static long CS[]= new long[DISK_RELATION_SIZE/PAGE_SIZE];
	static long CA[]= new long[DISK_RELATION_SIZE/PAGE_SIZE];
	static long CIO[]= new long[DISK_RELATION_SIZE/PAGE_SIZE];
	static long CH[]= new long[DISK_RELATION_SIZE/PAGE_SIZE];
	static int streamInputSize[]=new int[DISK_RELATION_SIZE/PAGE_SIZE];
	int queueStatus[]=new int[DISK_RELATION_SIZE/PAGE_SIZE];
	long waitingTime[]=new long [DISK_RELATION_SIZE];
	static int CE_index=0;
	public static int CS_index=0;
	static int CA_index=0;
	static int CIO_index=0;
	static int CH_index=0;
	static int pt_index=0;
	static int input_index=0;
	static int queue_index=0;
	static int rt_index=0;
	static int bl_index=0;
	static int WT_index=0;

	float oneNodeSize=0,memoryForFiftyTuples=0;
	boolean measurementStart=false;
	double sumOfFrequency,random,rawFK,minimumLimit;
	HybridJoin(){
		
	}
	
	public Connection connectDB(){
		Connection conn=null;
		try{
		
			String userName = "root";
			String password = "sunshine";
			String url = "jdbc:mysql://localhost/testdata";
			Class.forName ("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection (url, userName, password);
			System.out.println("Connected to Database");
		}
		catch (Exception e)
		{
			System.err.println (e);
		}
		return conn;
	}
	
	public void closeConnection(Connection conn){
		try{
			if(conn!=null){
				conn.close();
				System.out.println("Database connection closed");
			}
		}catch (SQLException e)
		{
			System.err.println (e);
		}
	}
	
	public static double integral(double limit){
		return (Math.log(limit));		//Exponent x^-1
	}
	
	public static double inverseIntegral(double x){
		return Math.exp(x);   //Inverse integral of exponent x^-1
	}	
	
	public void fillHashTable(){
		int tuples=0;
		sumOfFrequency=integral(MAX_KEY)-integral(MIN_KEY);
		minimumLimit=integral(MIN_KEY);
		random=myRandom.nextDouble();
		rawFK = inverseIntegral(random*sumOfFrequency+minimumLimit);
		streamRandomValue=(int)rawFK;
		head=new DoubleLinkQueue(streamRandomValue);
		currentNode=head;
		mhm.put(new Integer(streamRandomValue),new HybridJoinObject(streamRandomValue,streamRandomValue,streamRandomValue,streamRandomValue,streamRandomValue,currentNode,-1));
		oneNodeSize=SizeOfAgent.fullSizeOf(head);
		while(tuples<HASH_SIZE){
			random=myRandom.nextDouble();
			rawFK = inverseIntegral(random*sumOfFrequency+minimumLimit);
			streamRandomValue=(int)rawFK;
			if(streamRandomValue>=1&& streamRandomValue<DISK_RELATION_SIZE){
				currentNode=currentNode.addNode(streamRandomValue);
				mhm.put(new Integer(streamRandomValue),new HybridJoinObject(streamRandomValue,streamRandomValue,streamRandomValue,streamRandomValue,streamRandomValue,currentNode,-1));
				tuples++;
				if(tuples==49){
					memoryForFiftyTuples=SizeOfAgent.fullSizeOf(mhm);
				
				}
			}
		}
	}
	
	public void probIntoHash(){
		long start=0,stop=0,CH_per_Iteration=0, CEH_per_Iteration=0,CEQ_per_Iteration=0;
		boolean firstNode=false,lastNode=false;
		int processedTuplesCount=0;
		int index=head.popNode();
		readDBPage(index);
		for(int row=0; row<PAGE_SIZE; row++){
			//Probing into H
			if(mhm.containsKey(diskBuffer[row][0])){
				start=System.nanoTime();
				list=(ArrayList<HybridJoinObject>)mhm.get(diskBuffer[row][0]);
				stop=System.nanoTime();
				if(measurementStart){
					CH_per_Iteration+=stop-start;
				}
				start=System.nanoTime();
				mhm.remove(diskBuffer[row][0]);
				stop=System.nanoTime();
				if(measurementStart){
					CEH_per_Iteration+=stop-start;
				}
				for(int listItem=0; listItem<list.size(); listItem++){
					firstNode=false;
					lastNode=false;
					//Remove expire tuples from Q and H
					DoubleLinkQueue deleteNodeAddress=list.get(listItem).nodeAddress1;
					if(deleteNodeAddress==head){
						head=deleteNodeAddress.getNext();
						firstNode=true;
					}
					if(deleteNodeAddress==currentNode){
						currentNode=deleteNodeAddress.getPrecede();
						lastNode=true;
					}
					start=System.nanoTime();
					deleteNodeAddress.deleteNode(firstNode,lastNode);
					stop=System.nanoTime();
					//Delay measurement in queue
					/*
					if(list.get(listItem).arrivalTime!=-1){
						averageInputTime+=list.get(listItem).arrivalTime;
						count++;
						executed=true;
					}
					*/
					if(measurementStart){
						CEQ_per_Iteration+=stop-start;
					}
					
					requiredTuplesCount++;
					
				}
				
				if(measurementStart){
					CEH_per_Iteration+=CEQ_per_Iteration/list.size();
					CEQ_per_Iteration=0;
				}
				processedTuplesCount++;
			}	
		}
		if(measurementStart){
			CH[CH_index++]=CH_per_Iteration/processedTuplesCount;
			CE[CE_index++]=CEH_per_Iteration/processedTuplesCount;
		}
		
	}
	
	public void readDBPage(int index){
		int row=0;
		long start=0,stop=0;
		//Read into disk buffer
		try{
			start=System.nanoTime();
			rs=stmt.executeQuery("SELECT * from r_10_million where attr1>="+index+" AND attr1<"+(index+PAGE_SIZE)+"");
			stop=System.nanoTime();
			if(measurementStart){
				CIO[CIO_index++]=stop-start;
			}
			while(rs.next()){
				for(int col=1; col<=30; col++){
					diskBuffer[row][col-1]=rs.getInt(col);
				}
				row++;
			}
		}catch(SQLException e){System.out.print(e);}
	}
		
	public void appendHash(){
		long start=0,stop=0,CA_per_Iteration=0;
		int eachInputSize=0;
		//Append into Q and H from stream buffer
		//Stream buffer waiting time
		while(streamBuffer.size()<requiredTuplesCount);
		System.out.println("requiredTuplesCount =" + requiredTuplesCount);
		while (requiredTuplesCount>0){
			/*		
			
			if(measurementStart){
				waitingTime[WT_index++]=System.currentTimeMillis()-streamBuffer.peek().arrivalTime;
			}
			 */
			start=System.nanoTime();
			currentNode=currentNode.addNode(streamBuffer.peek().attr1);
			mhm.put(new Integer(streamBuffer.peek().attr1),new HybridJoinObject(streamBuffer.peek().attr1,streamBuffer.peek().attr2,streamBuffer.peek().attr3,streamBuffer.peek().attr4,streamBuffer.peek().attr5,currentNode,System.currentTimeMillis()));
			streamBuffer.poll();
			stop=System.nanoTime();
			if(measurementStart){
				CA_per_Iteration+=stop-start;
			}
		
			requiredTuplesCount--;
			eachInputSize++;
		}
		System.out.println("Stream After:  "+streamBuffer.size());	
		if(measurementStart){
		
			CA[CA_index++]=CA_per_Iteration/eachInputSize;
			streamInputSize[input_index++]=eachInputSize;			
		}
		
	}
	

public static void main(String args[])throws java.io.IOException, InterruptedException{
	HybridJoin hj=new HybridJoin();
	HybridjoinStartUpdatesStream stream=new HybridjoinStartUpdatesStream();
	System.out.println("Hybrid Join in execution mode...");
	hj.fillHashTable();
	Connection conn=hj.connectDB();
	try{
		hj.stmt=conn.createStatement();
		hj.stmt.setFetchSize(PAGE_SIZE);
	}catch(SQLException e){System.out.print(e);}
	
	stream.start();
	for(int round=1; round<=2; round++){
		for(int tuple=1; tuple<=DISK_RELATION_SIZE; tuple+=PAGE_SIZE){
			hj.measurementStart=false;
			if(round==1){
				hj.measurementStart=true;
			}
			hj.probIntoHash();
			hj.appendHash();
		}
	}
	stream.stop();
	System.out.println("Hash tuples: "+mhm.size());
	System.out.println("\nMEMORY COST");
	float Hash=(HASH_SIZE*(hj.memoryForFiftyTuples/50))/1048576f;
	float Queue=(hj.oneNodeSize*QUEUE_SIZE)/1048576f;
	float bufferb=SizeOfAgent.fullSizeOf(diskBuffer)/1048576f;
	System.out.println("Memory used by Hash Table:  "+Hash+"  MB");
	System.out.println("Memory used by Queue:  "+Queue+" MB");
	System.out.println("Memory used by buffer b :  "+bufferb+"  MB");
	System.out.println("pt_index"+HybridJoin.pt_index+"CH_index: "+HybridJoin.CH_index+" CS_Index: "+HybridJoin.CS_index+" CA_index: "+HybridJoin.CA_index+" CE_index: "+HybridJoin.CE_index+" CIO_index: "+HybridJoin.CIO_index);
	hj.closeConnection(conn);
	
	//To calculate the access frequency of R
	/*
	hj.frequency=hj.CAPFR.getFrequency();
	double R=0.5;
	//System.out.println("LastPage\t2ndLastPage\t3rdLastPage\t4thLastPage\t5thLastPage\tLast20Pages");
	for(int i=0; i<5; i++){
		System.out.println("Frequency of last pages when R = "+R+" Milions");
		for(int j=0; j<6; j++){
			System.out.print(hj.frequency[i][j]+"\t");
		}
		R+=R;
	}
	*/
	System.out.println("\nStream tuples in back log:"+HybridJoin.streamBuffer.size());
	System.out.println("Room for more tuples:"+hj.requiredTuplesCount);
	System.out.println("Queue status:"+hj.head.countNodes());
	
	BufferedWriter bw=new BufferedWriter(new FileWriter("d://workspace//result//Cloop_cost_lamda_2000_M=50MB_R=10M_exp=-1_DB_500_.txt"));
/*
	bw.write("Time to stay each tuple in Hybrid Join Window");
	bw.newLine();
	for(int i=0; i<hj.WT_index; i++){
		bw.write(hj.waitingTime[i]+"");
		bw.newLine();
	}
*/	
	
	bw.write("HYBRIDJOIN PROCESSING COST");
	bw.newLine();
	bw.write("Input(w)     CH(NSec)\t    CS(NSec)\t    CA(NSec)\t      CE(NSec)\t    CIO(NSec)");
	bw.newLine();
	
	for(int i=0; i<HybridJoin.CIO_index; i++){
		bw.write(HybridJoin.streamInputSize[i]+"\t\t");
		bw.write(HybridJoin.CH[i]+"\t\t");
		bw.write(HybridJoin.CS[i]+"\t\t");
		bw.write(HybridJoin.CA[i]+"\t\t");
		bw.write(HybridJoin.CE[i]+"\t\t");
		bw.write(HybridJoin.CIO[i]+"");
		bw.newLine();
	}
	bw.close();
	System.out.println("\nPROCESSING COST for HYBRID join has been written in specified text file");
	
}
}	
	
	
	
		
	
	