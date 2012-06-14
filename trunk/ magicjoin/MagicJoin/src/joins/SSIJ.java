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
import stream.HybridjoinStartUpdatesStream;
/**
 * This program measures the waiting time and processing time in HYBRIDJOIN. In addition this also
 * calculates the processing cost for each loop iteration that is used to calculate the
 * service rate. 
 * @author gxia003
 *
 */
public class SSIJ {
	private static final int QUEUE_SIZE = 0;
	//this buffer is used to store incoming update tuples before load them into the Input Buffer
	public static LinkedBlockingQueue<MeshJoinObject> inBuffer=new LinkedBlockingQueue<MeshJoinObject>();
	//Input Buffer (use ArrayBlockingQueue due to its size

		
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
		onlinePhase();
	}
	
	public void onlinePhase(){
		/**
		 * check if the join attribute is cached for update tuples
		 * if yes, join immediately
		 * if no, store tuples into stream buffer for the join phase
		 */
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

