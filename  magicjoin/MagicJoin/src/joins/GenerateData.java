package joins;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.Random;

import java.sql.Connection;


/**
 * @author gxia003
 *This class supply methods to generate 16000000 test data, which includes attribute:
 *id,name,age,city
 */
public class GenerateData {
	
	//25 city names
	public static String[] city={"Bath","Brimingham","Bradford","Brighton and Hove","Bristol","Cambridge","Canterbury",
			"Carlisle","Chester","Chichester","Coventry","Derby","Durham","Ely","Exeter","Gloucester","Coventry",
			"Leeds","Manchester","Ripon","York","Wells","Oxford","Truro","London"};
	
	//25 customer names
	public static String[] name={"Smith","Anderson","Clark","Wright","Johnson","Thomas","Martin","Lewis","Grant","King",
			"Taylor","robinson","Allen","Lee","Ronald","Mary","Nancy","Helen","Lisa","Elizabeth","Jeff",
			"Donald","Anthony","Michael","James"};
	
	
	public static void createRPJOIN(Connection con) throws SQLException{
		Statement stmt=null;
		System.out.println("create processing...");
		String query="CREATE TABLE RPJOIN2 "
				+ "(attr1 INT UNSIGNED NOT NULL AUTO_INCREMENT,"
				+ "attr2 INT UNSIGNED,"
				+ "attr3 INT UNSIGNED,"
				+ "attr4 INT UNSIGNED,"
				+ "attr5 INT UNSIGNED,"
				+ "attr6 INT UNSIGNED,"
				+ "attr7 INT UNSIGNED,"
				+ "attr8 INT UNSIGNED,"
				+ "attr9 INT UNSIGNED,"
				+ "attr10 INT UNSIGNED,"
				+ "attr11 INT UNSIGNED,"
				+ "attr12 INT UNSIGNED,"
				+ "attr13 INT UNSIGNED,"
				+ "attr14 INT UNSIGNED,"
				+ "attr15 INT UNSIGNED,"
				+ "attr16 INT UNSIGNED,"
				+ "attr17 INT UNSIGNED,"
				+ "attr18 INT UNSIGNED,"
				+ "attr19 INT UNSIGNED,"
				+ "attr20 INT UNSIGNED,"
				+ "attr21 INT UNSIGNED,"
				+ "attr22 INT UNSIGNED,"
				+ "attr23 INT UNSIGNED,"
				+ "attr24 INT UNSIGNED,"
				+ "attr25 INT UNSIGNED,"
				+ "attr26 INT UNSIGNED,"
				+ "attr27 INT UNSIGNED,"
				+ "attr28 INT UNSIGNED,"
				+ "attr29 INT UNSIGNED,"
				+ "attr30 INT UNSIGNED,"
				+ "PRIMARY KEY(attr1)) ENGINE=MyISAM";
		try{		
			stmt=con.createStatement();
			stmt.executeUpdate(query);
			System.out.println("table should be created.");
		}catch (SQLException e){
		e.printStackTrace();
		}finally{
			if (stmt!=null) {stmt.close();}
		}
	}
	
	
	public static void insertRecord(int i, Connection con) throws SQLException{
		Statement stmt=null;
		String query="INSERT INTO RPJOIN2(attr2,attr3,attr4,attr5,attr6,attr7,attr8,attr9,attr10,attr11,attr12,attr13,attr14,attr15,attr16,attr17,attr18,attr19,attr20,attr21,attr22,attr23,attr24,attr25,attr26,attr27,attr28,attr29,attr30) VALUES"
				+ "(" +i+","+i+","+i+","+i+","+i+","+i+","+i+","+i+","+i+","
				+i+","+i+","+i+","+i+","+i+","+i+","+i+","+i+","+i+","+i+","
				+i+","+i+","+i+","+i+","+i+","+i+","+i+","+i+","+i+","+i+")";
		try{		
			stmt=con.createStatement();
			stmt.executeUpdate(query);
			System.out.println("The "+i+"th new customer record is added");
			
		}catch (SQLException e){
		e.getMessage(); //need more detailed here
		}finally{
			if (stmt!=null) {stmt.close();}
		}
	}
	
	/*public static void createCustomerTable(Connection con) throws SQLException{
		Statement stmt=null;
		System.out.println("create processing...");
		String query="CREATE TABLE Customer"
				+ "(ID MEDIUMINT UNSIGNED NOT NULL AUTO_INCREMENT,"
				+ "Name varchar(50),"
				+ "Age TINYINT UNSIGNED,"
				+ "City varchar(50),"
				+ "PRIMARY KEY(ID)) ENGINE=MyISAM";
		try{		
			stmt=con.createStatement();
			stmt.executeUpdate(query);
			System.out.println("table should be created.");
		}catch (SQLException e){
		e.printStackTrace();
		}finally{
			if (stmt!=null) {stmt.close();}
		}
	}
	
	
	public static void insertCustomer(int i, Connection con, String name, int age, String city) throws SQLException{
		Statement stmt=null;
		String query="INSERT INTO Customer (Name,age,city) VALUES"
				+ "('"+ name +"',"+ age +",'"+city +"')";
		try{		
			stmt=con.createStatement();
			stmt.executeUpdate(query);
			System.out.println("The "+i+"th new customer record is added");
			
		}catch (SQLException e){
		e.getMessage(); //need more detailed here
		}finally{
			if (stmt!=null) {stmt.close();}
		}
	}*/

	/*public static void showPet(Connection con) throws SQLException{
		Statement stmt=null;
		String query1="SELECT * from pet;";
		try{		
			stmt=con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
			ResultSet rs=stmt.executeQuery(query1);
			
		while(rs.next()){
				String name=rs.getString("name");
				String owner=rs.getString("owner");
				String species=rs.getString("species");
				String sex=rs.getString("sex");
				Date birth=rs.getDate("birth");
				Date death=rs.getDate("death");
				if(name.length()<7)
				System.out.println(name+"\t\t"+owner+"\t"+species+"\t"+sex+"\t"+birth+"\t"+death);
				else System.out.println(name+"\t"+owner+"\t"+species+"\t"+sex+"\t"+birth+"\t"+death);
		}

		}catch (SQLException e){
		e.getMessage(); //need more detailed here
		}finally{
			if (stmt!=null) {stmt.close();}
		}
	}*/
	
	public static Connection getConnection(String user, String pwd) throws SQLException{
		
			Connection conn=null;
			Properties connectionProps=new Properties();
			connectionProps.put("user", user);
			connectionProps.put("password", pwd);
		try {	
			conn=DriverManager.getConnection("jdbc:mysql://localhost/testdata",connectionProps);
			System.out.println("Connection established");
		} catch (Exception e) {
			System.err.println("problem on connection stage");
		}
		return conn;
	}
	
	public static void main(String[] args){
		Connection con=null;
/*		Random rd1=new Random();
		Random rd2=new Random();
		Random rd3=new Random();*/
		try {
			con=getConnection("root","sunshine");
		} catch (SQLException e) {
			e.printStackTrace();
			System.err.println("Failed to connect.");
		}
		try {
			    createRPJOIN(con);
				/*for(int i=0;i<16000000;i++){
				int rc=rd1.nextInt(25);
				int rn=rd2.nextInt(25);
				int ra=16+rd3.nextInt(50);
				insertCustomer(i,con, name[rn],ra,city[rc]);				
			    }*/
			  
				for(int i=1;i<2000001;i++){
						insertRecord(i,con);				
				}
			System.out.println("we here");
			
		} catch (SQLException e) {
			e.printStackTrace();
			System.err.println("Failed to create table.");
		}
	}
	
}
