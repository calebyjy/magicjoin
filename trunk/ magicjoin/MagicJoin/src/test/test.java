package test;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.GradientPaint;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.CategoryAxis;
import org.jfree.chart.axis.CategoryLabelPositions;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.renderer.category.BarRenderer;
import org.jfree.data.category.CategoryDataset;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.ui.ApplicationFrame;
import org.jfree.ui.RefineryUtilities;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * A simple demonstration application showing how to create a bar chart.
 *
 */
public class test extends ApplicationFrame {
	
	Statement stmt1;
	Statement stmt2;
	Statement stmt3;
	Statement stmt4;
	Statement stmt5;
	Statement stmt6;
	Statement stmt7;
	Statement stmt8;
	
	public Connection connectDB(){
		Connection conn=null;
		Properties connectionProps=new Properties();
		connectionProps.put("user", "root");
		connectionProps.put("password", "sunshine");
		try {	
			conn=DriverManager.getConnection("jdbc:mysql://localhost/testdata",connectionProps);
			System.out.println("Connection established");
		} catch (Exception e) {
			System.err.println("problem on connection stage");
		}
		return conn;
	}
	
	public void closeConnection(Connection con){
		try{
			if(con!=null){
				con.close();
				System.out.println("Database connection closed");
			}
		}catch (SQLException e)
		{
			System.err.println (e);
		}
	}

    public test(final String title) {

        super(title);
        Connection con=connectDB();
		try {
			stmt1=con.createStatement();
			stmt2=con.createStatement();
			stmt3=con.createStatement();
			stmt4=con.createStatement();
			stmt5=con.createStatement();
			stmt6=con.createStatement();
			stmt7=con.createStatement();
			stmt8=con.createStatement();
			
		} catch (SQLException e1) {
			e1.printStackTrace();
		}

        final CategoryDataset dataset = createDataset();
        final JFreeChart chart = createChart(dataset);
        final ChartPanel chartPanel = new ChartPanel(chart);
        chartPanel.setPreferredSize(new Dimension(1000, 600));
        setContentPane(chartPanel);

    }


    private CategoryDataset createDataset() {
        
        // row keys...
        final String series1 = "number of tuples";
        //final String series2 = "Second";
        //final String series3 = "Third";

        // column keys...
        final String category1 = "<10sec";
        final String category2 = "10-30sec";
        final String category3 = "10-60sec";
        final String category4 = "60-120sec";
        final String category5 = "120-240sec";
        final String category6 = "240-480sec";
        final String category7 = "480-960sec";
        final String category8 = ">960sec";    
        int count1=0;
        int count2=0;
        int count3=0;
        int count4=0;
        int count5=0;
        int count6=0;
        int count7=0;
        int count8=0;
        

        // create the dataset...
        final DefaultCategoryDataset dataset = new DefaultCategoryDataset();
        
        try{
			ResultSet rs1 = stmt1.executeQuery("SELECT count(*) from complete_rpjoin_r2million_d500_p500_w2000_j500 where processing_time>0 and processing_time<10000");
			ResultSet rs2 = stmt2.executeQuery("SELECT count(*) from complete_rpjoin_r2million_d500_p500_w2000_j500 where processing_time>10000 and processing_time<30000");
			ResultSet rs3 = stmt3.executeQuery("SELECT count(*) from complete_rpjoin_r2million_d500_p500_w2000_j500 where processing_time>30000 and processing_time<60000");
			ResultSet rs4 = stmt4.executeQuery("SELECT count(*) from complete_rpjoin_r2million_d500_p500_w2000_j500 where processing_time>60000 and processing_time<120000");
			ResultSet rs5 = stmt5.executeQuery("SELECT count(*) from complete_rpjoin_r2million_d500_p500_w2000_j500 where processing_time>120000 and processing_time<240000");
			ResultSet rs6 = stmt6.executeQuery("SELECT count(*) from complete_rpjoin_r2million_d500_p500_w2000_j500 where processing_time>240000 and processing_time<480000");
			ResultSet rs7 = stmt7.executeQuery("SELECT count(*) from complete_rpjoin_r2million_d500_p500_w2000_j500 where processing_time>480000 and processing_time<960000");
			ResultSet rs8 = stmt8.executeQuery("SELECT count(*) from complete_rpjoin_r2million_d500_p500_w2000_j500 where processing_time>960000 ");

			rs1.next();
			rs2.next();
			rs3.next();
			rs4.next();
			rs5.next();
			rs6.next();
			rs7.next();
			rs8.next();
			
			count1=rs1.getInt(1);	
			count2=rs2.getInt(1);
			count3=rs3.getInt(1);
			count4=rs4.getInt(1);
			count5=rs5.getInt(1);
			count6=rs6.getInt(1);
			count7=rs7.getInt(1);
			count8=rs8.getInt(1);
			
			System.out.println("<10secs: "+count1);
			System.out.println("10-30secs: "+count1);
			System.out.println("30-60secs: "+count3);
			System.out.println("60-120secs: "+count4);
			System.out.println("120-240secs: "+count5);
			System.out.println("240-480secs: "+count6);
			System.out.println("480-960secs: "+count7);
			System.out.println(">960secs: "+count8);

		}catch(SQLException e){System.out.print(e);}

        dataset.addValue(count1, series1, category1);
        dataset.addValue(count2, series1, category2);
        dataset.addValue(count3, series1, category3);
        dataset.addValue(count4, series1, category4);
        dataset.addValue(count5, series1, category5);
        dataset.addValue(count6, series1, category6);
        dataset.addValue(count7, series1, category7);
        dataset.addValue(count8, series1, category8);

        /*dataset.addValue(5.0, series2, category1);
        dataset.addValue(7.0, series2, category2);
        dataset.addValue(6.0, series2, category3);
        dataset.addValue(8.0, series2, category4);
        dataset.addValue(4.0, series2, category5);

        dataset.addValue(4.0, series3, category1);
        dataset.addValue(3.0, series3, category2);
        dataset.addValue(2.0, series3, category3);
        dataset.addValue(3.0, series3, category4);
        dataset.addValue(6.0, series3, category5);*/
        
        return dataset;
        
    }
    
    /**
     * Creates a sample chart.
     * 
     * @param dataset  the dataset.
     * 
     * @return The chart.
     */
    private JFreeChart createChart(final CategoryDataset dataset) {
        
        // create the chart...
        final JFreeChart chart = ChartFactory.createBarChart(
            "processing time distribution",         // chart title
            "range of processing time",               // domain axis label
            "number of tuples",                  // range axis label
            dataset,                  // data
            PlotOrientation.VERTICAL, // orientation
            true,                     // include legend
            true,                     // tooltips?
            false                     // URLs?
        );

        // NOW DO SOME OPTIONAL CUSTOMISATION OF THE CHART...

        // set the background color for the chart...
        chart.setBackgroundPaint(Color.white);

        // get a reference to the plot for further customisation...
        final CategoryPlot plot = chart.getCategoryPlot();
        plot.setBackgroundPaint(Color.lightGray);
        plot.setDomainGridlinePaint(Color.white);
        plot.setRangeGridlinePaint(Color.white);

        // set the range axis to display integers only...
        final NumberAxis rangeAxis = (NumberAxis) plot.getRangeAxis();
        rangeAxis.setStandardTickUnits(NumberAxis.createIntegerTickUnits());

        // disable bar outlines...
        final BarRenderer renderer = (BarRenderer) plot.getRenderer();
        renderer.setDrawBarOutline(false);
        
        // set up gradient paints for series...
        final GradientPaint gp0 = new GradientPaint(
            0.0f, 0.0f, Color.blue, 
            0.0f, 0.0f, Color.lightGray
        );
        final GradientPaint gp1 = new GradientPaint(
            0.0f, 0.0f, Color.green, 
            0.0f, 0.0f, Color.lightGray
        );
        final GradientPaint gp2 = new GradientPaint(
            0.0f, 0.0f, Color.red, 
            0.0f, 0.0f, Color.lightGray
        );
        //renderer.setSeriesPaint(0, gp0);
        //renderer.setSeriesPaint(1, gp1);
        //renderer.setSeriesPaint(2, gp2);

        final CategoryAxis domainAxis = plot.getDomainAxis();
        domainAxis.setCategoryLabelPositions(
            CategoryLabelPositions.createUpRotationLabelPositions(Math.PI / 6.0)
        );
        // OPTIONAL CUSTOMISATION COMPLETED.
        
        return chart;
        
    }
    
    // ****************************************************************************
    // * JFREECHART DEVELOPER GUIDE                                               *
    // * The JFreeChart Developer Guide, written by David Gilbert, is available   *
    // * to purchase from Object Refinery Limited:                                *
    // *                                                                          *
    // * http://www.object-refinery.com/jfreechart/guide.html                     *
    // *                                                                          *
    // * Sales are used to provide funding for the JFreeChart project - please    * 
    // * support us so that we can continue developing free software.             *
    // ****************************************************************************
    
    /**
     * Starting point for the demonstration application.
     *
     * @param args  ignored.
     */
    public static void main(final String[] args) {

        final test demo = new test("Bar Chart Demo");
        demo.pack();
        RefineryUtilities.centerFrameOnScreen(demo);
        demo.setVisible(true);

    }

}