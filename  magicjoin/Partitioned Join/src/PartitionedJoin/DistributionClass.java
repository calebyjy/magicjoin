package PartitionedJoin;
import java.util.Random;
/**
 * This program generates the distribution based on Zipf's Law with exponent -1 and 
 *implements the market rule 20/80.
 * @author asif
 *
 */
public class DistributionClass {
	public final static int MIN_KEY=1;
	public final static int MAX_KEY=8000000;
	public double sumOfFrequency=0.0,random=0.0,rawValue=0.0;
	int value=0;
	
	Random myRandom=new Random();
	DistributionClass(){
	}
	
	public int getNextDistributionValue(){	
		sumOfFrequency=integral(MAX_KEY)-integral(MIN_KEY);
		random=myRandom.nextDouble();
		rawValue=inverseIntegral(random*sumOfFrequency+integral(MIN_KEY));
		value=(int)rawValue;
		return(value);
	}
	
	public static double integral(double limit){
		return (Math.log(limit));		//Exponent x^-1
	}
	
	public static double inverseIntegral(double x){
		return Math.exp(x);   //Inverse integral of exponent x^-1
	}
}	
	