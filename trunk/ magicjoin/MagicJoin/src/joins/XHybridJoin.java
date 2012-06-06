package extendedHybridJoin;
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

/**
 * This program implements the algorithm for X-HYBRIDJOIN. In addition this also
 * calculates the processing cost for each loop iteration that is used to calculate the
 * service rate. 
 * @author asif
 *
 */

public class XHybridJoin {
	public static final int HASH_SIZE=512567;
	public static final int QUEUE_SIZE=HASH_SIZE;
	public static final int STREAM_SIZE=5000;
	public static final int DISK_RELATION_SIZE=2000000;
	public static final int  =1500;
	public static final int SWAP_DB=950;
	public static final int MIN_KEY=1;
	public static final int MAX_KEY=DISK_RELATION_SIZE;
	
	//***************MEASUREMENT LIMITS************
	public static final int MEASUREMENT_START=1;
	public static final int MEASUREMENT_STOP=DISK_RELATION_SIZE;
		
	static MultiMap<Integer,HybridJoinObject> mhm=new MultiHashMap<Integer,HybridJoinObject>();
	static ArrayList <HybridJoinObject> list=new ArrayList<HybridJoinObject>();
	static LinkedBlockingQueue<HybridJoinObject> streamBuffer=new LinkedBlockingQueue<HybridJoinObject>();
	static int diskBuffernonvolatile[][]=new int[NON__SWAP_DB][30];
	static int diskBuffervolatile[][]=new int[SWAP_DB][30];
	
	Random myRandom=new Random();
	Statement stmt=null;
	ResultSet rs=null;
	Queue head,currentNode;
	int streamRandomValue;
	int requiredTuplesCount=0,non_vola=0,vola=0;
	static long CE[]= new long[DISK_RELATION_SIZE/100];
	static long CS[]= new long[DISK_RELATION_SIZE/100];
	static long CA[]= new long[DISK_RELATION_SIZE/100];
	static long CIO[]= new long[DISK_RELATION_SIZE/100];
	static long CH[]= new long[DISK_RELATION_SIZE/100];
	static int streamInputSize[]=new int[DISK_RELATION_SIZE/100];
	static int Non_swappable[]=new int[DISK_RELATION_SIZE/100];
	static int non_swappable_index=0,CE_index=0,CS_index=0,CA_index=0,CIO_index=0,CH_index=0,pt_index=0,input_index=0,queue_index=0,rt_index=0,bl_index=0,WT_index=0;
	float oneNodeSize=0,memoryForFiftyTuples=0;
	boolean measurementStart=false;
	double sumOfFrequency,random,rawFK,minimumLimit;
	
	XHybridJoin(){
		
	}
	/***
	 * Establish the connection with databases 
	 * @return
	 */
	
	public Connection connectDB(){
		Connection conn=null;
		try{
		
			String userName = "root";
			String password = "root";
			String url = "jdbc:mysql://localhost/jdbctest";
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
	
	/***
	 * Terminate the databases connection
	 * @param conn
	 */
	
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
	
	/***
	 * Implements the integral of x^-1
	 * @param limit
	 * @return
	 */
	
	public static double integral(double limit){
		return (Math.log(limit));	
	}
	
	/***
	 * /***
	 * Implements the inverse integral of x^-1
	 * @param limit
	 * @return
	 */
	
	public static double inverseIntegral(double x){
		return Math.exp(x);   
		
	}	
	
	/***
	 * This method fill the hash table using Zipf's distribution
	 */
	
	public void fillHashTable(){
		int tuples=0;
		sumOfFrequency=integral(MAX_KEY)-integral(MIN_KEY);
		minimumLimit=integral(MIN_KEY);
		random=myRandom.nextDouble();
		rawFK = inverseIntegral(random*sumOfFrequency+minimumLimit);
		streamRandomValue=(int)rawFK;
		head=new Queue(streamRandomValue);
		currentNode=head;
		mhm.put(new Integer(streamRandomValue),new HybridJoinObject(streamRandomValue,streamRandomValue,streamRandomValue,streamRandomValue,streamRandomValue,currentNode));
		oneNodeSize=SizeOfAgent.fullSizeOf(head);
		while(tuples<HASH_SIZE){
			random=myRandom.nextDouble();
			rawFK = inverseIntegral(random*sumOfFrequency+minimumLimit);
			streamRandomValue=(int)rawFK;
			if(streamRandomValue>=1&& streamRandomValue<DISK_RELATION_SIZE){
				currentNode=currentNode.addNode(streamRandomValue);
				mhm.put(new Integer(streamRandomValue),new HybridJoinObject(streamRandomValue,streamRandomValue,streamRandomValue,streamRandomValue,streamRandomValue,currentNode));
				tuples++;
				if(tuples==49){
					memoryForFiftyTuples=SizeOfAgent.fullSizeOf(mhm);
				
				}
			}
		}
		
	}
	/***
	 * In this method algorithm read one-by-one all tuples from both parts of
	 * the disk buffer (swappable and non-swappable)and look-up them into hash table.
	 *  If tuple is matched it generate that tuple as an output tuple and delete it
	 *  from hash table along with dequeuing it from queue. For each matching the
	 *  algorithm also increments the variable requiredTuplesCount by one which will be
	 *  the stream input size for next iteration of the algorithm.
	 *  Apart from that in this method the look-up cost for both parts of the disk buffer,
	 *  output generating cost and tuples removing cost from hash and queue are also measured.   
	 * 
	 */
		
	public void probIntoHash(){
		
		long start=0,stop=0,CH_per_Iteration=0,CEH_per_Iteration=0,CEQ_per_Iteration=0;
		boolean firstNode=false,lastNode=false;
		int processedTuplesCount=0;
		int hashProbCount=0;
		non_vola=0; //to count the matching tuples for non-swappable part separately
		//Probing of non-swappable part of the disk buffer
		for(int row=0; row<NON__SWAP_DB; row++){
			if(mhm.containsKey(diskBuffernonvolatile[row][0])){
				start=System.nanoTime();
				list=(ArrayList<HybridJoinObject>)mhm.get(diskBuffernonvolatile[row][0]);
				stop=System.nanoTime();
				hashProbCount++;
				if(measurementStart){
					CH_per_Iteration+=stop-start;
				}
				start=System.nanoTime();
				mhm.remove(diskBuffernonvolatile[row][0]);
				stop=System.nanoTime();
				if(measurementStart){
					CEH_per_Iteration+=stop-start;
				}
				for(int listItem=0; listItem<list.size(); listItem++){
					firstNode=false;
					lastNode=false;
					Queue deleteNodeAddress=list.get(listItem).nodeAddress;
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
					if(measurementStart){
						CEQ_per_Iteration+=stop-start;
						non_vola++;
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
		
		
		int index=head.popNode();
		readDBvolatilePage(index);
		
		//Probing of swappable part of disk buffer
		for(int row=0; row<SWAP_DB; row++){
			
			if(mhm.containsKey(diskBuffervolatile[row][0])){
				start=System.nanoTime();
				list=(ArrayList<HybridJoinObject>)mhm.get(diskBuffervolatile[row][0]);
				stop=System.nanoTime();
				hashProbCount++;
				if(measurementStart){
					CH_per_Iteration+=stop-start;
				}
				start=System.nanoTime();
				mhm.remove(diskBuffervolatile[row][0]);
				stop=System.nanoTime();
				if(measurementStart){
					CEH_per_Iteration+=stop-start;
				}
				for(int listItem=0; listItem<list.size(); listItem++){
					firstNode=false;
					lastNode=false;
					Queue deleteNodeAddress=list.get(listItem).nodeAddress;
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
					if(measurementStart){
						CEQ_per_Iteration+=stop-start;
						vola++;
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
			CH[CH_index++]=CH_per_Iteration/hashProbCount;
			CE[CE_index++]=CEH_per_Iteration/processedTuplesCount;
			Non_swappable[non_swappable_index++]=non_vola;
		}
		
	}
	
	/**
	 *In this method the algorithm reads the first disk page of  R into the non-swappable
	 *part of the disk buffer. This method executes only once in the beginning of the
	 *algorithm. The IO cost to read the disk page is also measured in this method 
	 * @param index
	 */
	public void readDBNonVolatilePage(){
		int row=0;
			
		try{
			rs=stmt.executeQuery("SELECT * from lookup_table_twomillion where attr1>="+1+" AND attr1<"+NON__SWAP_DB+";");
			while(rs.next()){
				for(int col=1; col<=30; col++){
					diskBuffernonvolatile[row][col-1]=rs.getInt(col);
				}
				row++;
			}
		}catch(SQLException e){System.out.print(e);}
	}
	
	/**
	 *In this method the algorithm reads the disk page into disk buffer using the 
	 *oldest value from queue as an index. The IO cost to read the disk page is also 
	 *measured in this method 
	 * @param index
	 */
	public void readDBvolatilePage(int index){
		int row=0;
		long start=0,stop=0;
		try{
			start=System.nanoTime();
			rs=stmt.executeQuery("SELECT * from lookup_table_twomillion where attr1>="+index+" AND attr1<"+(index+SWAP_DB)+";");
			stop=System.nanoTime();
			if(measurementStart){
				CIO[CIO_index++]=stop-start;
			}
			while(rs.next()){
				for(int col=1; col<=30; col++){
					diskBuffervolatile[row][col-1]=rs.getInt(col);
				}
				row++;
			}
		}catch(SQLException e){System.out.print(e);}
	}
	
	/**
	 * In this method the algorithm checks the availability of stream.
	 * If update stream available the algorithm reads the input stream equal to 
	 * the size of requiredTuplesCount from stream buffer and loads it into hash table
	 * along with storing their join attribute values into the queue. Once the stream input
	 * is read the algorithm reset the variable requiredTuplesCount to zero. The reading cost
	 * for the required stream input size is also measured in this method.     
	 */	
	public void appendHash(){
		long start=0,stop=0,CA_per_Iteration=0;
		int eachInputSize=0;
	
		while(streamBuffer.size()<requiredTuplesCount);
		
		while (requiredTuplesCount>0){
			start=System.nanoTime();
			currentNode=currentNode.addNode(streamBuffer.peek().attr1);
			mhm.put(new Integer(streamBuffer.peek().attr1),new HybridJoinObject(streamBuffer.peek().attr1,streamBuffer.peek().attr2,streamBuffer.peek().attr3,streamBuffer.peek().attr4,streamBuffer.peek().attr5,currentNode));
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
	/**
	 * In main method, the automatic generation of stream is started by
	 * calling the method stream.start(). Once the stream gets start, the algorithm
	 * one by one calls the above defined methods appropriately. At the end when the
	 * measurement time finishes the algorithm starts writing all measured processing 
	 * costs into the specified text file.
	 * @param args
	 * @throws java.io.IOException
	 * @throws InterruptedException
	 */

public static void main(String args[])throws java.io.IOException, InterruptedException{
	XHybridJoin hj=new XHybridJoin();
	StartUpdatesStream stream=new StartUpdatesStream();
	System.out.println("Hybrid Join in execution mode...");
	hj.fillHashTable();
	Connection conn=hj.connectDB();
	try{
		hj.stmt=conn.createStatement();
		hj.stmt.setFetchSize(SWAP_DB);
		System.out.println("Fetch Size: "+hj.stmt.getFetchSize());
	}catch(SQLException e){System.out.print(e);}
	
	stream.start();
	//Thread.sleep(20000);
	hj.readDBNonVolatilePage();
	for(int round=1; round<=2; round++){
		for(int tuple=1; tuple<=DISK_RELATION_SIZE; tuple+=SWAP_DB){
			hj.measurementStart=false;
			if((round==1)&& (tuple>10*SWAP_DB)){
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
	float bufferW=SizeOfAgent.fullSizeOf(streamBuffer)/1048576f;
	float bufferb=SizeOfAgent.fullSizeOf(diskBuffervolatile)/1048576f;
	float total=Hash+Queue+bufferW+bufferb;
	
	System.out.println("Memory used by Hash Table:  "+Hash+"  MB");
	System.out.println("Memory used by Queue:  "+Queue+" MB");
	System.out.println("Memory used by Stream buffer:  "+bufferW+" MB");
	System.out.println("Memory used by buffer b :  "+bufferb+"  MB");
	System.out.println("Total Memory: "+total+" MB");
	System.out.println("pt_index"+hj.pt_index+"CH_index: "+hj.CH_index+" CS_Index: "+hj.CS_index+" CA_index: "+hj.CA_index+" CE_index: "+hj.CE_index+" CIO_index: "+hj.CIO_index);
	hj.closeConnection(conn);
	
	System.out.println("Queue status:"+hj.head.countNodes());
	System.out.println("Non Volatile: "+hj.non_vola);
	System.out.println("Volatile: "+hj.vola);
	BufferedWriter bw=new BufferedWriter(new FileWriter("F://Tuned-X-HYBRIDJOIN//Tuning_of_swaapble_part_M_varies//Memory_100MB//Cost_for_nswpdb_1500_swpdb_950_M_100MB_R_2Million_Expo_-1.txt"));

	bw.write("Detail of processing cost when N-SWPDB=1500 tuples SWPDB=950 tuples, R=2Million, M=100MB and Exponent=-1 and Reading start after 10 iterations");
	bw.newLine();
	bw.write("Non-swappable (w)\t Total(w)     CH(NSec)\t    CS(NSec)\t    CA(NSec)\t      CE(NSec)\t    CIO(NSec)");
	bw.newLine();
	
	for(int i=0; i<hj.CIO_index; i++){
		bw.write(XHybridJoin.Non_swappable[i]+"\t\t");
		bw.write(XHybridJoin.streamInputSize[i]+"\t\t");
		bw.write(XHybridJoin.CH[i]+"\t\t");
		bw.write(XHybridJoin.CS[i]+"\t\t");
		bw.write(XHybridJoin.CA[i]+"\t\t");
		bw.write(XHybridJoin.CE[i]+"\t\t");
		bw.write(XHybridJoin.CIO[i]+"");
		bw.newLine();
	}
	bw.close();
	System.out.println("\nPROCESSING COST for HYBRID join has been written in specified text file");
	
}
}	
	
	
	
		
	
	