package joins;

import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
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
import objects.SSIJObject;
import stream.HybridjoinStartUpdatesStream;
/**
 * This program measures the waiting time and processing time for the Semi-Stream Index Join. In addition this also
 * calculates the processing cost for each loop iteration that can be used to calculate the service rate. 
 * @author gxia003
 *
 */
public class SSIJ {
	public static final int DISK_RELATION_SIZE=20000000;
	public static final int STREAM_SIZE=50000;
	public static final int IB_THRESH=4000;
	public static final int SB_THRESH=22000;

	
	//this buffer is used to store incoming update tuples before load them into the Input Buffer
	public static LinkedBlockingQueue<SSIJObject> sBuffer=new LinkedBlockingQueue<SSIJObject>();
	//Input Buffer 
	public static BPlusTree IB=new BPlusTree(6);
	//Stream Buffer
	public static BPlusTree SB=new BPlusTree(6);
	//Cached Relation
	public static BPlusTree CR=new BPlusTree(6);
	

		
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
	
	
	/**
	 * TODO: add benchmark condition and end-of-stream singal
	 */
	public void pendingPhase(){
		int sbSize=sBuffer.size();
		for(int i=0; i<sbSize; i++){
			SSIJObject so=sBuffer.poll();
			IB.insertOrUpdate(so.attr1, so);
		}
		if(IB.size>IB_THRESH){
			onlinePhase();
		}
	}
	
	public void onlinePhase(){
		/**
		 * check if the join attribute is cached for update tuples
		 * if yes, join immediately
		 * if no, store tuples into stream buffer for the join phase
		 */
		for(int i=0; i<IB.size; i++){
			
		}
	}
	
	public void joinPhase(){
		//read tuples from join buffer
		
	}
	
	public boolean isDeadline(){
		return false;
	}
	
	
	

	public static void main(String args[])throws java.io.IOException, InterruptedException{
	
	}
	
}	

