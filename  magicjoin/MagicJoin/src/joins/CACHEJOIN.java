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

import objects.HybridJoinDiskObject;
import objects.HybridJoinObject;

import org.apache.commons.collections15.MultiMap;
import org.apache.commons.collections15.multimap.MultiHashMap;
import sizeof.agent.SizeOfAgent;
import stream.HybridjoinStartUpdatesStream;

/**
 * This program implements our CACHEJOIN algorithm. It calculates the processing cost for
 * both modules of the algorithm. The major costs involve in the algorithms are:
 * Cost to look-up one tuple into the hash table (nano secs)= cH
 * Cost to generate the output for one tuple (nano secs)=cO
 * Cost to remove one tuple from the hash table and the queue (nano secs)=cE
 * Cost to read one stream tuple into the stream buffer (nano secs)=cS
 * Cost to append one tuple in the hash table and the queue (nano secs)=cA
 * Cost to calculate the frequency of one tuple in the disk buffer (nano secs)=cF
 * Total cost for one loop iteration of the algorithm (secs)=cloop
 * After the complete execution of the algorithm the program writes all above costs
 * into a text file whose path is needed to provide in the main program. 
 * @author asif
 *
 */

public class CACHEJOIN {
	public static final int HASH_SIZE=255017;
	public static final int QUEUE_SIZE=HASH_SIZE;
	public static final int STREAM_SIZE=5000;
	public static final int DISK_RELATION_SIZE=10000000;
	public static final int SWAP_DB=850;
	public static final int MIN_KEY=1;
	public static final int MAX_KEY=DISK_RELATION_SIZE;
	public static final int THRESHOLD=1;

	//***************MEASUREMENT LIMITS************
	public static final int MEASUREMENT_START=1;
	public static final int MEASUREMENT_STOP=DISK_RELATION_SIZE;

	static MultiMap<Integer,HybridJoinObject> mhm=new MultiHashMap<Integer,HybridJoinObject>();
	static ArrayList <HybridJoinObject> list=new ArrayList<HybridJoinObject>();
	public static LinkedBlockingQueue<HybridJoinObject> streamBuffer=new LinkedBlockingQueue<HybridJoinObject>();
	static int diskBuffervolatile[][]=new int[SWAP_DB][30];
	static int frequencyDetector[]=new int[SWAP_DB];

	Random myRandom=new Random();
	static Statement stmt=null;
	static ResultSet rs=null;
	DoubleLinkQueue head;
	public static DoubleLinkQueue currentNode;
	DoubleLinkQueue deleteNodeAddress;
	static DiskHashTableManipulation dhtm=null;
	int streamRandomValue;
	int requiredTuplesCount=0,non_vola=0,vola=0;
	static int tuplesMatchedIntoDiskHash=0;
	static long CE[]= new long[DISK_RELATION_SIZE/100];
	static long CS[]= new long[DISK_RELATION_SIZE/100];
	static long CA[]= new long[DISK_RELATION_SIZE/100];
	static long CIO[]= new long[DISK_RELATION_SIZE/100];
	static long CH[]= new long[DISK_RELATION_SIZE/100];
	static long CF[]= new long[DISK_RELATION_SIZE/100];
	static int streamInputSize[]=new int[DISK_RELATION_SIZE/100];
	static int StreamSizeMatchedInDiskHash[]=new int[DISK_RELATION_SIZE/100];
	static int accesed_page,CE_index=0,CS_index=0,CA_index=0,CIO_index=0,CH_index=0,pt_index=0,input_index=0,queue_index=0,rt_index=0,bl_index=0,WT_index=0,CF_index=0;
	float oneNodeSize=0,memoryForFiftyTuples=0;
	boolean measurementStart=false;
	double sumOfFrequency,random,rawFK,minimumLimit;

	int ir=52124,increment=12458,prime=2000003;

	CACHEJOIN()throws java.io.IOException{
		for(int i=0; i<frequencyDetector.length; i++){
			frequencyDetector[i]=0;
		}
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


	public void fillHashTable(){

		int tuples=0;
		sumOfFrequency=integral(MAX_KEY)-integral(MIN_KEY);
		minimumLimit=integral(MIN_KEY);
		random=myRandom.nextDouble();
		rawFK = inverseIntegral(random*sumOfFrequency+minimumLimit);
		streamRandomValue=(int)rawFK;
		head=new DoubleLinkQueue(streamRandomValue);
		currentNode=head;
		mhm.put(new Integer(streamRandomValue),new HybridJoinObject(streamRandomValue,streamRandomValue,streamRandomValue,streamRandomValue,streamRandomValue,currentNode, System.currentTimeMillis()));
		oneNodeSize=SizeOfAgent.fullSizeOf(head);
		while(tuples<HASH_SIZE){
			random=myRandom.nextDouble();
			rawFK = inverseIntegral(random*sumOfFrequency+minimumLimit);
			streamRandomValue=(int)rawFK;
			if(streamRandomValue>=1&& streamRandomValue<DISK_RELATION_SIZE){
				currentNode=currentNode.addNode(streamRandomValue);
				mhm.put(new Integer(streamRandomValue),new HybridJoinObject(streamRandomValue,streamRandomValue,streamRandomValue,streamRandomValue,streamRandomValue,currentNode,System.currentTimeMillis()));
				tuples++;
				if(tuples==49){
					memoryForFiftyTuples=SizeOfAgent.fullSizeOf(mhm);

				}
			}
		}

	}

	public boolean probIntoHash(){

		long start=0,stop=0,CH_per_Iteration=0,CEH_per_Iteration=0,CEQ_per_Iteration=0,CF_per_iteration=0;
		boolean firstNode=false,lastNode=false,tupleInMD=true;
		int processedTuplesCount=0,hashProbCount=0,detectedTupleCount=0;
		int index=head.popNode();
		System.out.println("processing tuple: "+ index);
		tupleInMD=readDBvolatilePage(index);
		System.out.println("tuple exist: "+ tupleInMD);

		if(tupleInMD){
			//Probing of disk buffer
			for(int row=0; row<SWAP_DB; row++){
				if(mhm.containsKey(diskBuffervolatile[row][1])){
					start=System.nanoTime();
					list=(ArrayList<HybridJoinObject>)mhm.get(diskBuffervolatile[row][1]);
					stop=System.nanoTime();
					hashProbCount++;
					if(measurementStart){
						CH_per_Iteration+=stop-start;
					}
					start=System.nanoTime();
					mhm.remove(diskBuffervolatile[row][1]); 
					stop=System.nanoTime();
					if(measurementStart){
						CEH_per_Iteration+=stop-start;
					}
					for(int listItem=0; listItem<list.size(); listItem++){
						System.out.println("Done by diskprobe: "+list.get(listItem).attr1);
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
						start=System.nanoTime();
						deleteNodeAddress.deleteNode(firstNode,lastNode);
						stop=System.nanoTime();
						if(measurementStart){
							CEQ_per_Iteration+=stop-start;
							vola++;
						}
						frequencyDetector[row]++;
						requiredTuplesCount++;
					}

					if(measurementStart){
						CEH_per_Iteration+=CEQ_per_Iteration/list.size();
						CEQ_per_Iteration=0;
					}
					processedTuplesCount++;
				}	
			}
			start=System.nanoTime();
			for(int row=0; row<SWAP_DB; row++){
				if(frequencyDetector[row]>=THRESHOLD&& DiskHashTableManipulation.dmhm.size()<DiskHashTableManipulation.NON_SWAP_DB){

					DiskHashTableManipulation.dmhm.put(diskBuffervolatile[row][1], new HybridJoinDiskObject(diskBuffervolatile[row][0],diskBuffervolatile[row][1],
							diskBuffervolatile[row][2],diskBuffervolatile[row][3],diskBuffervolatile[row][4],diskBuffervolatile[row][5],diskBuffervolatile[row][6],
							diskBuffervolatile[row][7],diskBuffervolatile[row][8],diskBuffervolatile[row][9],diskBuffervolatile[row][10],diskBuffervolatile[row][11],
							diskBuffervolatile[row][12],diskBuffervolatile[row][13],diskBuffervolatile[row][14],diskBuffervolatile[row][15],diskBuffervolatile[row][16],
							diskBuffervolatile[row][17],diskBuffervolatile[row][18],diskBuffervolatile[row][19],diskBuffervolatile[row][20],diskBuffervolatile[row][21],
							diskBuffervolatile[row][22],diskBuffervolatile[row][23],diskBuffervolatile[row][24],diskBuffervolatile[row][25],diskBuffervolatile[row][26],
							diskBuffervolatile[row][27],diskBuffervolatile[row][28],diskBuffervolatile[row][29]));
					detectedTupleCount++;
					//If you want to see which tuples is switched in H_R, just uncomment the following statement.
					//System.out.println(diskBuffervolatile[row][1]+  "is switch to disk hash");
					System.out.println("Cache loaded: "+ diskBuffervolatile[row][1]);
				}
				frequencyDetector[row]=0;				
				
			}
			
			System.out.println("Cache updated & required tuple= " + requiredTuplesCount );
			
			stop=System.nanoTime();

			if(measurementStart){
				CF[CF_index++]=stop-start;
				CH[CH_index++]=CH_per_Iteration/hashProbCount;
				CE[CE_index++]=CEH_per_Iteration/processedTuplesCount;
			}
		}
		return tupleInMD;
	}

	public boolean readDBvolatilePage(int index){
		int row=0,PageStart;
		long start=0,stop=0;
		boolean firstNode=false,lastNode=false,tupleInMD=true;
		//Loading of disk buffer
		try{
			start=System.nanoTime();
			//rs=stmt.executeQuery("Select attr1 FROM r_2_million USE INDEX(non_clustered) WHERE attr2="+index+"");
			rs=stmt.executeQuery("Select attr1 FROM r_10_million WHERE attr1="+index+"");

			if(!rs.next()){
				list=(ArrayList<HybridJoinObject>)mhm.get(index);
				mhm.remove(index);
				int count=0;
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
				System.out.println("required attribute doesn't exist.");
				tupleInMD=false;
			}
			else{
				PageStart=rs.getInt(1);
				rs=stmt.executeQuery("SELECT * from r_10_million where attr1>="+PageStart+" AND attr1<"+(PageStart+SWAP_DB)+"");
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
			}
		}catch(SQLException e){System.out.print(e);}
		return tupleInMD;
	}

	public void appendHash(){
		long start=0,stop=0,CA_per_Iteration=0;
		int eachInputSize=0;
		while(streamBuffer.size()<requiredTuplesCount){
			System.out.println("stream buffer size: " +streamBuffer.size());
			System.out.println("requiredTuplesCount:" + requiredTuplesCount);
		};
		tuplesMatchedIntoDiskHash=0;
		System.out.println("appendHash running");

		while (requiredTuplesCount>0){
			if(streamBuffer.size()<1){continue;}
			if(dhtm.matchedIntoDiskHash(streamBuffer.peek().attr1)){
				streamBuffer.poll();
				tuplesMatchedIntoDiskHash++;	
				//System.out.println("Done by Cache: "+ streamBuffer.peek().attr1);
			}
			else{
				start=System.nanoTime();
				currentNode=currentNode.addNode(streamBuffer.peek().attr1);
				mhm.put(new Integer(streamBuffer.peek().attr1),new HybridJoinObject(streamBuffer.peek().attr1,streamBuffer.peek().attr2,streamBuffer.peek().attr3,streamBuffer.peek().attr4,streamBuffer.peek().attr5,currentNode,System.currentTimeMillis()));
				streamBuffer.poll();
				stop=System.nanoTime();
				if(measurementStart){
					CA_per_Iteration+=stop-start;
				}
				requiredTuplesCount--;
				System.out.println("stream buffer size after join: " +streamBuffer.size());
				System.out.println("requireTuples after filling: "+ requiredTuplesCount);
				eachInputSize++;
			}
		}
		System.out.println("done by cache!!!");
		System.out.print("Stream After: " + streamBuffer.size());
		/*If you want to see which tuples matched in H_R and the total input size for next iteration
			 just uncomment the following statements.
		 */	
		//System.out.println("wH: "+tuplesMatchedIntoDiskHash);
		//System.out.println("w: "+eachInputSize);

		if(measurementStart){
			CA[CA_index++]=CA_per_Iteration/eachInputSize;
			streamInputSize[input_index]=eachInputSize;
			StreamSizeMatchedInDiskHash[input_index++]=tuplesMatchedIntoDiskHash;
		}
	}

	public static void main(String args[])throws java.io.IOException, InterruptedException{

		CACHEJOIN hj=new CACHEJOIN();
		HybridjoinStartUpdatesStream stream=new HybridjoinStartUpdatesStream();
		dhtm=new DiskHashTableManipulation();
		boolean tupleInMD=true;
		System.out.println("Hybrid Join in execution mode...");
		Connection conn=hj.connectDB();
		try{
			CACHEJOIN.stmt=conn.createStatement();
			CACHEJOIN.stmt.setFetchSize(SWAP_DB);
			System.out.println("Fetch Size: "+CACHEJOIN.stmt.getFetchSize());
		}catch(SQLException e){System.out.print(e);}

		hj.fillHashTable();
		stream.start();
		Thread.sleep(2000);
		for(int round=1; round<=2; round++){
			if(round==2){
				System.out.println("ROUND 2 Started...");
				System.out.println("Disk hash tuple: "+DiskHashTableManipulation.dmhm.size());
				//Thread.sleep(700);
			}
			for(int tuple=1; tuple<=DISK_RELATION_SIZE; tuple+=SWAP_DB){
				hj.measurementStart=false;
				if((round==2)&& (tuple>10*SWAP_DB)){
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
		System.out.println("Disk hash tuple: "+DiskHashTableManipulation.dmhm.size());
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
		System.out.println("pt_index"+hj.pt_index+"CH_index: "+hj.CH_index+" CS_Index: "+hj.CS_index+" CA_index: "+hj.CA_index+" CE_index: "+hj.CE_index+" CIO_index: "+hj.CIO_index+"CF_index+ "+hj.CF_index);
		hj.closeConnection(conn);

		System.out.println("Queue status:"+hj.head.countNodes());
		System.out.println("Non Volatile: "+hj.non_vola);
		System.out.println("Volatile: "+hj.vola);
		BufferedWriter bw=new BufferedWriter(new FileWriter("d://grant//workspace//result//Processing_Cost_swpdb_850_nswpdb_2500_M_50MB_R_10M_Expo_0.txt"));

		bw.write("Geralized X-HYBRIDJOIN PROCESSING COST WHEN SWPDB=850 TUPLES, N-SWPDB=2500 TUPLES STORED IN HASH, M=50MB, R=8 Millions and EXPONENT=-0.1");
		bw.newLine();

		bw.write("Total w\t w processed by disk Hash	w processed by disk buffer     CH(NSec)\t    CS(NSec)\t    CA(NSec)\t      CF\t	CE(NSec)\t    CIO(NSec)");
		bw.newLine();

		for(int i=0; i<CACHEJOIN.CIO_index; i++){
			bw.write((CACHEJOIN.StreamSizeMatchedInDiskHash[i]+CACHEJOIN.streamInputSize[i])+"\t\t");
			bw.write(CACHEJOIN.StreamSizeMatchedInDiskHash[i]+"\t\t");
			bw.write(CACHEJOIN.streamInputSize[i]+"\t\t");
			bw.write(CACHEJOIN.CH[i]+"\t\t");
			bw.write(CACHEJOIN.CS[i]+"\t\t");
			bw.write(CACHEJOIN.CA[i]+"\t\t");
			bw.write(CACHEJOIN.CF[i]+"\t\t");
			bw.write(CACHEJOIN.CE[i]+"\t\t");
			bw.write(CACHEJOIN.CIO[i]+"");
			bw.newLine();
		}

		bw.close();
		System.out.println("\nExecution has been completed");
	}
}	





