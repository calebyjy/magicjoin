package joins;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import sizeof.agent.*;
import stream.HybridjoinStartUpdatesStream;
import objects.MeshJoinObject;

import org.apache.commons.collections15.MultiMap;
import org.apache.commons.collections15.multimap.MultiHashMap;
import java.util.Random;
/**
 * In this program we implemented the prototype of MESHJOIN algorithm. The distribution
 *  of memory to the different components of MESHJOIN is according to the formulas we mentioned in the paper. However you
 *  have to set these values for every new memory budget for MESHJOIN.
 * The further specification of components is as under:
 * 1. DISK_TUPLES
 * : These are the total number of tuples in relation R, stored on disk (Database) and accessed sequentially but
 * in cyclic manner. In our implementation we have used 0.5 to 8 million tuples
 * 2. DISK-BUFFER
 * We have tuned the disk-buffer for every new budget using program "FindOptimalbForMeshJoin.java" and used the optimal value for disk-buffer for each scenario.
 * 3. STREAM-BUFFER
 * The stream-buffer is used to scan the input from stream S for each iteration of the algorithm and its size depends 
 * on the total memory budget allocated for algorithm.
 * 4. HASH_TUPLES
 * The total number of hash tuples can be calculated by multiplying the stream-buffer size with the total number of iterations
 * required to bring whole R into memory.
 * 5. ArraBlockingQueue
 * This is the queue to store the pointers addresses of the tuples in hash table in order to keep the record of expired tuples.
 * The number of cells in the queue is equal to the number of iterations and the size of each cell is depend on 
 * the size of stream-buffer.
 * 
 * For the moment the algorithm completes the two rounds of scanning the relation R and we measured the result on starting the 
 * second round. 
 *  
 * @author asif
 *
 */

//This program measure the time that each tuple has to wait before going to output
public class MeshJoinWithRinDB {
	//****************RELATION R******************
	public static final int R_SIZE=2000000;
	public static final int DISK_BUFFER=6069;
	public static final int QUEUE_SIZE=330;

	//************STREAM-BUFFER********************
	public static final int STREAM_BUFFER=4835;

	//***************MEASUREMENT LIMITS************
	//public static final int LATE_START=40000;
	public static final int MEASUREMENT_START=1;
	public static final int MEASUREMENT_STOP=R_SIZE;
	public static final int MEASUREMENT_SIZE=MEASUREMENT_STOP-MEASUREMENT_START;

	static MultiMap<Integer,MeshJoinObject> mhm=new MultiHashMap<Integer,MeshJoinObject>();
	static ArrayBlockingQueue<Object> abq=new ArrayBlockingQueue<Object>(QUEUE_SIZE);
	static LinkedBlockingQueue<MeshJoinObject> streamBuffer=new LinkedBlockingQueue<MeshJoinObject>();
	static int bufferB[][]=new int[DISK_BUFFER][30];
	static ArrayList <MeshJoinObject> list=new ArrayList<MeshJoinObject>();

	Connection conn=null;
	Statement stmt=null;
	ResultSet rs=null;

	BufferedReader in;
	int unmatched=0;
	int startRead;

	static long CE[]= new long[MEASUREMENT_SIZE+100];
	static long CS[]= new long[MEASUREMENT_SIZE+100];
	static long CA[]= new long[MEASUREMENT_SIZE+100];
	static long CIO[]= new long[MEASUREMENT_SIZE+100];
	static long CH[]= new long[MEASUREMENT_SIZE];
	static long waitingTime[]=new long[MEASUREMENT_SIZE];
	static int CE_index=0,CS_index=0,CA_index=0,CIO_index=0,CH_index=0,WT_index=0;

	boolean round2=false,measurementStart=false;
	double sumOfFrequency=0,minimumLimit=0,random=0,rawFK=0;
	int streamRandomValue=0;
	Random myRandom=new Random();
	HybridjoinStartUpdatesStream stream=new HybridjoinStartUpdatesStream();

	MeshJoinWithRinDB(){

	}



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




	/**
	 * This method set the pointer addresses of incoming stream tuples in the queue
	 * @param w
	 * @param key
	 * @param index
	 */
	public void set(int w[],int key,int index){
		w[index]=key;
	}

	/**
	 * This method get the pointer addresses from queue to identify the expired
	 * tuples from the hash table
	 * @param w
	 * @param index
	 * @return
	 */
	public int get(int w[],int index){
		return w[index];
	}

	/**
	 * This method sequentially read the fixed amount of tuples from the disk using disk_buffer
	 * and probe them in hash table. 
	 * @param ht
	 * @throws java.sql.SQLException
	 */
	public void lookupMasterData()throws java.io.IOException, InterruptedException{

		long startTime=0,endTime=0,CH_per_Iteration=0;

		int matched=0,diskInputs=0;

		conn=connectDB();
		mhm.clear();
		int index=0;
		try{
			stmt=conn.createStatement();
			stmt.setFetchSize(DISK_BUFFER);

		}catch(SQLException exp){exp.printStackTrace();}

		stream.start();
		//Thread.sleep(LATE_START);
		for(int round=1; round<=2; round++){
			startRead=1;
			if(round==2){
				System.out.println("Second round started");
				round2=true;
			}
			for(int tuple=0; tuple<R_SIZE; tuple++){

				measurementStart=false;
				if((round2)&&(tuple>=MEASUREMENT_START && tuple<MEASUREMENT_STOP)){
					measurementStart=true;
				}
				if(tuple%DISK_BUFFER==0){
					readDiskTuplesIntoBufferB();
					inputStream();
					diskInputs++;
					if(measurementStart && matched!=0){
						CH[CH_index++]=CH_per_Iteration/matched;
						CH_per_Iteration=0;matched=0;
					}

					index=0;
				}


				if(mhm.containsKey(bufferB[index][0])){
					startTime=System.nanoTime();
					list=(ArrayList<MeshJoinObject>)mhm.get(bufferB[index][0]);
					endTime=System.nanoTime();
					if(measurementStart){
						CH_per_Iteration+=endTime-startTime;
						matched++;
					}
				}
				index++;
			}
		}

		stream.stop();
		closeConnection(conn);
		System.out.println("Hash tuples:  "+mhm.size());
		System.out.println("Unmatched tuples: "+unmatched);
		System.out.println("Iterations required to bring R into Disk_buffer="+diskInputs);
		System.out.println("Stream back log: "+streamBuffer.size());

	}	

	/**
	 * This method generate the input stream and load it into stream buffer.
	 * Before loading the stream tuples into hash table it check the status of queue
	 * if it is already full remove the oldest tuples from hash table along with
	 * their pointer addresses from Queue. Finally it loads the new scanned tuples 
	 * into hash table with their pointer address in queue.
	 * @param ht
	 */
	public void inputStream(){
		long startTime=0,endTime=0,CA_per_Iteration=0;;
		int w[]=new int[STREAM_BUFFER];

		if(round2){
			removeExpireTuples();
		}
		while(streamBuffer.size()<STREAM_BUFFER);
		for(int i=0; i<STREAM_BUFFER; i++){
			startTime=System.nanoTime();
			set(w,streamBuffer.peek().attr1,i);
			mhm.put(new Integer(streamBuffer.peek().attr1),streamBuffer.poll());
			endTime=System.nanoTime();
			if(measurementStart){
				CA_per_Iteration+=endTime-startTime;
			}
		}
		System.out.println("Available stream size After: "+streamBuffer.size());
		abq.offer(w);
		if(measurementStart){
			CA[CA_index++]=CA_per_Iteration/STREAM_BUFFER;
		}
	}

	/**
	 * This method remove the oldest tuples from hash table along with their addresses
	 * @param ht
	 */

	public void removeExpireTuples(){
		long startTime=0,endTime=0,CE_per_Iteration=0;
		int w[]=(int[])abq.poll();
		for(int i=0; i<STREAM_BUFFER; i++){
			startTime=System.nanoTime();
			if(mhm.containsKey(new Integer(get(w,i)))){
				mhm.remove(new Integer(get(w,i)));
				unmatched++;
			}
			endTime=System.nanoTime();
			CE_per_Iteration+=endTime-startTime;
		}
		CE[CE_index++]=CE_per_Iteration/DISK_BUFFER;
	}

	/**
	 * This method read the given number of tuples from disk to disk_buffer. 
	 * @throws java.io.IOException
	 */
	public void readDiskTuplesIntoBufferB()throws java.io.IOException{
		long startTime=0,endTime=0;
		int row=0;
		startTime=System.nanoTime();
		try{
			startTime=System.nanoTime();
			rs=stmt.executeQuery("SELECT * from lookup_table_twomillion where attr1>="+startRead+" AND attr1<"+(startRead+DISK_BUFFER)+"");
			endTime=System.nanoTime();
			if(measurementStart){
				CIO[CIO_index++]=endTime-startTime;
			}
			while(rs.next()){
				for(int col=1; col<=30; col++){
					bufferB[row][col-1]=rs.getInt(col);
				}
				row++;
			}

		}catch(SQLException e){System.out.print(e);}

		startRead+=DISK_BUFFER;

	}


	/**
	 * The main method creates the object of the class and call the above mentioned 
	 * methods as per requirement.
	 * @param args
	 * @throws java.io.IOException
	 */
	public static void main (String args[])throws java.io.IOException, InterruptedException{

		MeshJoinWithRinDB mjwfm=new MeshJoinWithRinDB();
		System.out.println("Processing Continue...");
		mjwfm.lookupMasterData();

		System.out.println("MESHJOIN With fixed memory");
		System.out.println("\nMEMORY STATUS");

		float Hash=(SizeOfAgent.fullSizeOf(mhm)/1048576f);
		float Queue=SizeOfAgent.fullSizeOf(abq)/1048576f;
		float bufferW=SizeOfAgent.fullSizeOf(streamBuffer)/1048576f;
		float bufferb=SizeOfAgent.fullSizeOf(bufferB)/1048576f;
		float total=Hash+Queue+bufferW+bufferb;

		System.out.println("Memory used by Hash Table:  "+Hash+"  MB");
		System.out.println("Memory used by Queue:  "+Queue+" MB");
		System.out.println("Memory used by Stream buffer:  "+bufferW+" MB");
		System.out.println("Memory used by buffer b :  "+bufferb+"  MB");
		System.out.println("Total Memory: "+total+" MB");
		BufferedWriter bw=new BufferedWriter(new FileWriter("F://Performance comp using realistic benchmark//MESHJOIN//M varies//Cloop_Cost_Lamda=2000_R=2m_M=250MB.txt"));
		System.out.println("\nTIME STATUS");
		bw.write("MESHJOIN PROCESSING COST");
		bw.newLine();
		bw.write("CH(NSec)\t   CS(NSec)\t     CA(NSec)\t    CE(NSec)\t       CIO(NSec)");
		bw.newLine();

		for(int i=0; i<MeshJoinWithRinDB.CIO_index; i++){
			bw.write(MeshJoinWithRinDB.CH[i]+"\t\t");
			bw.write(MeshJoinWithRinDB.CS[i]+"\t\t");
			bw.write(MeshJoinWithRinDB.CA[i]+"\t\t");
			bw.write(MeshJoinWithRinDB.CE[i]+"\t\t");
			bw.write(MeshJoinWithRinDB.CIO[i]+"");
			bw.newLine();
		}
		bw.close();

		System.out.println("\nDelay for MESHJOIN has been written in specified text file");
	}

}
