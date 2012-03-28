package joins;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.io.BufferedWriter;
import java.io.FileWriter;

/**
 * This program implements the Index Nested Loop Join (INLJ) and measure the processing
 * cost for each tuple. On the basis of this cost we can calculate the service rate for
 * INLJ
 * @author asif
 *
 */
public class INLJoin {
	public static final int STREAM_TUPLES=317677;
	public static final int DISK_RELATION_SIZE=8000000;
	
	INLJoin(){
		
	}
	
	private Connection connectDB(){
		Connection conn=null;
		try{
			String userName = "root";
			String password = "root";
			String url = "jdbc:mysql://localhost/jdbctest";
			Class.forName ("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection (url, userName, password);
		}
    catch (Exception e)
    {
        System.err.println (e);
    }
		return conn;
	}
	
	private void clloseConnection(Connection conn){
		try{
			if(conn!=null)
				conn.close();
		}catch (SQLException e)
	    {
	        System.out.println(e);
	    }
	}
	
	private void executeJoin(Connection conn)throws java.io.IOException{
		int streamRandomValue,n=1,seed=215683,rp=7941;
		int streamTuple[]=new int[5];
		long timebefore=0,timeafter=0,totalProcessingTime=0;
		long processingCost[]=new long[STREAM_TUPLES];
		BufferedWriter bw=new BufferedWriter(new FileWriter("C://eclipsProjects//MyWork//INL_processingCost//INL_Cost_8M.txt"));
		int count=0;
		try{
			ResultSet rs=null;
			Statement stmt=conn.createStatement();
			stmt.setFetchSize(1);
			
			for(int i=0; i<STREAM_TUPLES; i++){
				timebefore=System.nanoTime();
				streamRandomValue=(seed+n++*rp)%(DISK_RELATION_SIZE-1);
				for(int j=0; j<5; j++){
					streamTuple[j]=streamRandomValue;
				}
				rs=stmt.executeQuery("SELECT * from lookup_table_eightmillion WHERE attr1="+streamTuple[0]+";");
				rs.setFetchSize(1);
				
				while(rs.next()){
					count++;
				}
				timeafter=System.nanoTime();
				processingCost[i]=timeafter-timebefore;
			}			
			
			for(int i=0; i<STREAM_TUPLES; i++){
				bw.write(processingCost[i]+"");
				bw.newLine();
				totalProcessingTime+=processingCost[i];
			}
			bw.close();
			System.out.println("Total matched tuples: "+count);
			totalProcessingTime/=1000000000;
			System.out.println("Processing time: "+totalProcessingTime+"  Seconds");
			}catch (Exception e)
			{
	        e.printStackTrace();
			}
	}

	
	public static void main(String args[])throws java.io.IOException{
		INLJoin inlj=new INLJoin();
		Connection conn=inlj.connectDB();
		System.out.println("Connected to DB");
		System.out.println("INL is in execution mode...");
		inlj.executeJoin(conn);
		inlj.clloseConnection(conn);
		System.out.println("DB Connection closed");
	}

}


