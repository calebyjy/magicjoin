package cacheJoinSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class GenerateRandomMasterData {
	private Connection connectDB(){
		Connection conn=null;
		try{

			String userName = "root";
			String password = "root";
			String url = "jdbc:mysql://localhost/masterdata";
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
			System.err.println (e);
		}
	}

	private void insertRecords(Connection conn){
		int count=0;
		int duplicateTest[]=new int[8000009];
		int ir=52124,increment=1225458,prime=8000009;
		for(int i=0; i<8000009; i++){
			duplicateTest[i]=0;
		}
		try{
			Statement stmt=conn.createStatement();
			while(count<8000000){
				ir=(ir+increment)%prime;
				if(duplicateTest[ir]==0){
					stmt.executeUpdate("INSERT INTO product_unsorted_8million VALUES("+count+","+ir+","+ir+","+ir+","+ir+","+ir+","+ir+","+ir+","+ir+","+ir+","+ir+","+ir+","+ir+","+ir+","+ir+","+ir+","+ir+","+ir+","+ir+","+ir+","+ir+","+ir+","+ir+","+ir+","+ir+","+ir+","+ir+","+ir+","+ir+","+ir+");");
					duplicateTest[ir]=ir;
					count++;
				}
				System.out.println("Count: "+count+"\tvalue: "+ir);		
			}
			System.out.println(count+"  tuples added into database");
		}catch (Exception e){
			System.err.println (e);
		}	
	}
	/*
	private void SelectRecord(Connection conn){
		try{
			Statement stmt=conn.createStatement();
			ResultSet rs=stmt.executeQuery("Select attr1 from product where attr2=1;");
			System.out.println(rs.next());
		}catch (Exception e){
			System.err.println (e);
		}	
	}
	 */
	public static void main(String args[]){
		GenerateRandomMasterData grmd=new GenerateRandomMasterData();
		Connection conn=grmd.connectDB();
		//grmd.SelectRecord(conn);
		grmd.insertRecords(conn);
		grmd.clloseConnection(conn);

	}
}
