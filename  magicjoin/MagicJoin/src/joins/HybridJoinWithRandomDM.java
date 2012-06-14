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

import objects.HybridJoinObject;

import org.apache.commons.collections15.MultiMap;
import org.apache.commons.collections15.multimap.MultiHashMap;
import sizeof.agent.SizeOfAgent;
import stream.HybridjoinStartUpdatesStream;
/**
 * This program implements the algorithm for HYBRIDJOIN on random Master Data. The program also
 * calculates the processing cost for each loop iteration that is used to calculate the
 * service rate. Major costs involve in HYBRIDJOIN algorithm are:
 * Cost to look-up one tuple into the hash table (nano secs)= cH
 * Cost to generate the output for one tuple (nano secs)=cO
 * Cost to remove one tuple from the hash table and the queue (nano secs)=cE
 * Cost to read one stream tuple into the stream buffer (nano secs)=cS
 * Cost to append one tuple in the hash table and the queue (nano secs)=cA
 * Total cost for one loop iteration of the algorithm (secs)=cloop
 * After the complete execution of the algorithm the program writes all above costs
 * into a text file whose path is needed to provide in the main program. 
 * @author asif
 *
 */
public class HybridJoinWithRandomDM {
	public static final int HASH_SIZE=256710;
	public static final int QUEUE_SIZE=HASH_SIZE;
	public static final int STREAM_SIZE=50000;
	public static final int DISK_RELATION_SIZE=8000000;
	public static final int PAGE_SIZE=500;
	public static final int MIN_KEY=1;
	public static final int MAX_KEY=DISK_RELATION_SIZE;

	//***************Measurements Limits************
	public static final int MEASUREMENT_START=1;
	public static final int MEASUREMENT_STOP=DISK_RELATION_SIZE;

	//***************Declaration and initialization of join components************************
	static MultiMap<Integer,HybridJoinObject> mhm=new MultiHashMap<Integer,HybridJoinObject>();
	static ArrayList <HybridJoinObject> list=new ArrayList<HybridJoinObject>();
	static LinkedBlockingQueue<HybridJoinObject> streamBuffer=new LinkedBlockingQueue<HybridJoinObject>();
	static int diskBuffer[][]=new int[PAGE_SIZE][30];
	Random myRandom=new Random();
	Statement stmt=null;
	ResultSet rs=null;
	DoubleLinkQueue head,currentNode,deleteNodeAddress;
	int streamRandomValue;
	int requiredTuplesCount=0;
	static long CE[]= new long[DISK_RELATION_SIZE/PAGE_SIZE];
	static long CS[]= new long[DISK_RELATION_SIZE/(PAGE_SIZE/2)];
	static long CA[]= new long[DISK_RELATION_SIZE/PAGE_SIZE];
	static long CIO[]= new long[DISK_RELATION_SIZE/PAGE_SIZE];
	static long CH[]= new long[DISK_RELATION_SIZE/PAGE_SIZE];
	static int streamInputSize[]=new int[DISK_RELATION_SIZE/PAGE_SIZE];
	int queueStatus[]=new int[DISK_RELATION_SIZE/PAGE_SIZE];
	long waitingTime[]=new long [DISK_RELATION_SIZE];
	static int CE_index=0,CS_index=0,CA_index=0,CIO_index=0,CH_index=0,pt_index=0,input_index=0,queue_index=0,rt_index=0,bl_index=0,WT_index=0;

	float oneNodeSize=0,memoryForFiftyTuples=0;
	boolean measurementStart=false;
	double sumOfFrequency,random,rawFK,minimumLimit;
	HybridJoinWithRandomDM(){

	}

	/***
	 * Establish the connection with databases 
	 * @return
	 */
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

	public static double integral(double limit){
		//return((100/99)*Math.pow(limit,0.99)); //Exponent 0.01=approx. equal to 0
		//return (4/3)*Math.pow(limit, 0.75); //Exponent 0.25
		//return (2*Math.sqrt(limit));	//Exponent 0.5
		//return(4*Math.pow(limit, 0.25)); //Exponent 0.75
		return (Math.log(limit));		//Exponent x^1
		//return(-4*Math.pow(limit,-0.25)); //Exponent 1.25
		//return(-2/Math.sqrt(limit));	//Exponent 1.5 
		//return(-3/4*(Math.pow(limit, -0.75))); //Exponent 1.75

	}

	public static double inverseIntegral(double x){
		//return Math.pow(0.99*x,1.0101); ////Inverse Integral of exponent 0.01=approx. equal to 0
		//return (Math.pow((0.75*x),4)/(x*x)); //Inverse integral of exponent 0.25
		//return (x*x)/4;   	//Inverse integral of exponent 0.05
		//return(Math.pow(x/4, 4)); //Inverse integral of exponent 0.75
		return Math.exp(x);   //Inverse integral of exponent 1
		//return(Math.pow((x/-4),-4)); //Inverse integral of exponent 1.25
		//return(4/(Math.pow(x, 2)));	//Inverse integral of exponent 1.5
		//return(Math.pow((-4/3)*x, (-4/3))); //Inverse Integral of exponent 1.75
	}


	/***
	 * This method fill the hash table using Zipfian distribution
	 */
	public void fillHashTable(){
		int tuples=0;
		sumOfFrequency=integral(MAX_KEY)-integral(MIN_KEY);
		minimumLimit=integral(MIN_KEY);
		random=myRandom.nextDouble();
		rawFK = inverseIntegral(random*sumOfFrequency+minimumLimit);
		streamRandomValue=(int)rawFK;
		head=new DoubleLinkQueue(streamRandomValue);
		currentNode=head;
		mhm.put(new Integer(streamRandomValue),new HybridJoinObject(streamRandomValue,streamRandomValue,streamRandomValue,streamRandomValue,streamRandomValue,currentNode,System.nanoTime()));
		oneNodeSize=SizeOfAgent.fullSizeOf(head);
		while(tuples<HASH_SIZE){
			random=myRandom.nextDouble();
			rawFK = inverseIntegral(random*sumOfFrequency+minimumLimit);
			streamRandomValue=(int)rawFK;
			if(streamRandomValue>=1&& streamRandomValue<DISK_RELATION_SIZE){
				currentNode=currentNode.addNode(streamRandomValue);
				mhm.put(new Integer(streamRandomValue),new HybridJoinObject(streamRandomValue,streamRandomValue,streamRandomValue,streamRandomValue,streamRandomValue,currentNode,System.nanoTime()));
				tuples++;
				if(tuples==49){
					memoryForFiftyTuples=SizeOfAgent.fullSizeOf(mhm);

				}
			}
		}
	}
	/***
	 * In this method algorithm read one-by-one all tuples of disk page from disk buffer
	 * and look-up them into hash table. If tuple is matched it generate that tuple as an
	 * output tuple and delete it from hash table along with dequeuing it from queue. 
	 * For each matching the algorithm also increments the variable requiredTuplesCount
	 *  by one which will be the stream input size for next iteration of the algorithm.
	 *  Apart from that in this method the look-up cost, output generating cost and tuples
	 *  removing cost from hash and queue are also measured.   
	 * 
	 */
	public boolean probIntoHash(){
		long start=0,stop=0,CH_per_Iteration=0,CEH_per_Iteration=0,CEQ_per_Iteration=0;
		boolean firstNode=false,lastNode=false, tupleInMD=true;
		int processedTuplesCount=0;
		int index=head.popNode();
		tupleInMD=readDBPage(index);
		if(tupleInMD){
			for(int row=0; row<PAGE_SIZE; row++){
				//Probing into H
				if(mhm.containsKey(diskBuffer[row][1])){
					start=System.nanoTime();
					list=(ArrayList<HybridJoinObject>)mhm.get(diskBuffer[row][1]);
					stop=System.nanoTime();
					if(measurementStart){
						CH_per_Iteration+=stop-start;
					}
					start=System.nanoTime();
					mhm.remove(diskBuffer[row][1]);
					stop=System.nanoTime();
					if(measurementStart){
						CEH_per_Iteration+=stop-start;
					}	
					for(int listItem=0; listItem<list.size(); listItem++){
						firstNode=false;
						lastNode=false;
						//Remove expire tuples from Q and H
						deleteNodeAddress=list.get(listItem).nodeAddress1;
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
			System.out.println("w: "+processedTuplesCount);
		}
		return tupleInMD;
	}
	/**
	 *In this method the algorithm reads the disk page into disk buffer using the 
	 *oldest value from queue as an index. The IO cost to read the disk page is also 
	 *measured in this method 
	 * @param index
	 */
	public boolean readDBPage(int index){
		int row=0,PageStart;
		long start=0,stop=0;
		boolean firstNode=false,lastNode=false,tupleInMD=true;
		//Read into disk buffer
		try{
			System.out.println("Index: "+index);
			start=System.nanoTime();
			rs=stmt.executeQuery("Select attr1 FROM r_10_million WHERE attr2="+index+"");
			if(!rs.next()){
				list=(ArrayList<HybridJoinObject>)mhm.get(index);
				mhm.remove(index);
				for(int listItem=0; listItem<list.size(); listItem++){
					firstNode=false;
					lastNode=false;
					deleteNodeAddress=list.get(listItem).nodeAddress1;
					if(deleteNodeAddress==head){
						head=deleteNodeAddress.getNext();
						firstNode=true;
					}
					if(deleteNodeAddress==currentNode){
						currentNode=deleteNodeAddress.getPrecede();
						lastNode=true;
					}
					deleteNodeAddress.deleteNode(firstNode,lastNode);
				}
				tupleInMD=false;
			}
			else{
				PageStart=rs.getInt(1);
				rs=stmt.executeQuery("SELECT * from r_10_million where attr1>="+PageStart+" AND attr1<"+(PageStart+PAGE_SIZE)+"");
				stop=System.nanoTime();
				System.out.println("attr1 at index: "+PageStart);
				if(measurementStart){
					CIO[CIO_index++]=stop-start;
				}
				while(rs.next()){
					for(int col=1; col<=30; col++){
						diskBuffer[row][col-1]=rs.getInt(col);
					}
					row++;
				}
			}
		}catch(SQLException e){System.out.print(e);}
		return tupleInMD;
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
		//Append into Q and H from stream buffer

		while(streamBuffer.size()<requiredTuplesCount);

		while (requiredTuplesCount>0){
			start=System.nanoTime();
			currentNode=currentNode.addNode(streamBuffer.peek().attr1);
			mhm.put(new Integer(streamBuffer.peek().attr1),new HybridJoinObject(streamBuffer.peek().attr1,streamBuffer.peek().attr2,streamBuffer.peek().attr3,streamBuffer.peek().attr4,streamBuffer.peek().attr5,currentNode,System.nanoTime()));
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
		HybridJoinWithRandomDM hj=new HybridJoinWithRandomDM();
		HybridjoinStartUpdatesStream stream=new HybridjoinStartUpdatesStream();
		boolean tupleInMD=true;
		System.out.println("Hybrid Join in execution mode...");
		hj.fillHashTable();
		Connection conn=hj.connectDB();
		BufferedWriter bw=new BufferedWriter(new FileWriter("d://workspace//result//HYBRIDJOIN_Cloop_cost_unsortedMD_lamda_2000_M_50MB_R_10_M_exp_-0.1_DB_500.txt"));
		bw.write("HYBRIDJOIN PROCESSING COST\n");
		try{
			hj.stmt=conn.createStatement();
			hj.stmt.setFetchSize(PAGE_SIZE);
		}catch(SQLException e){System.out.print(e);}

		stream.start();
		for(int round=1; round<=2; round++){
			for(int tuple=1; tuple<=DISK_RELATION_SIZE; tuple+=PAGE_SIZE){
				hj.measurementStart=false;
				if((round==2)&&(tuple>10*PAGE_SIZE)){
					hj.measurementStart=true;
				}
				tupleInMD=hj.probIntoHash();
				if(tupleInMD){
					hj.appendHash();
				}
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
		System.out.println("pt_index"+HybridJoinWithRandomDM.pt_index+"CH_index: "+HybridJoinWithRandomDM.CH_index+" CS_Index: "+HybridJoinWithRandomDM.CS_index+" CA_index: "+HybridJoinWithRandomDM.CA_index+" CE_index: "+HybridJoinWithRandomDM.CE_index+" CIO_index: "+HybridJoinWithRandomDM.CIO_index);
		hj.closeConnection(conn);
		System.out.println("\nStream tuples in back log:"+HybridJoinWithRandomDM.streamBuffer.size());
		System.out.println("Room for more tuples:"+hj.requiredTuplesCount);
		System.out.println("Queue status:"+hj.head.countNodes());

		bw.write("While specifications are,DB=500 tuples, R=8 Million, M=50MB and Exponent=-0.1 and Reading start after 10 iterations of 2nd round");
		bw.newLine();
		bw.write("Input(w)     CH(NSec)\t    CS(NSec)\t    CA(NSec)\t      CE(NSec)\t    CIO(NSec)");
		bw.newLine();

		for(int i=0; i<HybridJoinWithRandomDM.CIO_index; i++){
			bw.write(HybridJoinWithRandomDM.streamInputSize[i]+"\t\t");
			bw.write(HybridJoinWithRandomDM.CH[i]+"\t\t");
			bw.write(HybridJoinWithRandomDM.CS[i]+"\t\t");
			bw.write(HybridJoinWithRandomDM.CA[i]+"\t\t");
			bw.write(HybridJoinWithRandomDM.CE[i]+"\t\t");
			bw.write(HybridJoinWithRandomDM.CIO[i]+"");
			bw.newLine();
		}
		bw.close();
		System.out.println("\nPROCESSING COST for HYBRID join has been written in specified text file");

	}
}	
	
	
	
		
	
	