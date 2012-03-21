package stream;

/**
 * This program calculate the access frequency for disk based relation R at different positions.
 * To calculate the access frequency is important because if we observe the real nature of daily
 * market transactions, a few product are sold out frequently while most rest are rarely,.
 * Therefore during join execution only a little part of R is accessed again and again while 
 * the rest part of R is accessed rarely.
 * To analyze this feature practically we have conducted this experiment
 * @author asif
 *
 */
public class CalculateAccessPageFrequencyOfR {
	int frequency[][]={{0,0,0,0,0,0},
					   {0,0,0,0,0,0},
					   {0,0,0,0,0,0},
					   {0,0,0,0,0,0},
					   {0,0,0,0,0,0}};
	int lastPage1=0,secondLastPage1=0,thirdLastPage1=0,fourthLastPage1=0,fifthLastPage1=0,lastTwentyPages1=0;
	int lastPage2=0,secondLastPage2=0,thirdLastPage2=0,fourthLastPage2=0,fifthLastPage2=0,lastTwentyPages2=0;
	int lastPage3=0,secondLastPage3=0,thirdLastPage3=0,fourthLastPage3=0,fifthLastPage3=0,lastTwentyPages3=0;
	int lastPage4=0,secondLastPage4=0,thirdLastPage4=0,fourthLastPage4=0,fifthLastPage4=0,lastTwentyPages4=0;
	int lastPage5=0,secondLastPage5=0,thirdLastPage5=0,fourthLastPage5=0,fifthLastPage5=0,lastTwentyPages5=0;
	int oneByFiveM=500000,oneM=1000000,twoM=2000000,fourM=4000000,eightM=8000000,pageSize=500;
	public void CalculateAccessPageFrequency(int streamRandomValue){
		
		//R=0.5 Million
		if(streamRandomValue>oneByFiveM-pageSize && streamRandomValue<=oneByFiveM){	
			lastPage1++;
		}
		if(streamRandomValue>oneByFiveM-2*pageSize && streamRandomValue<=oneByFiveM-pageSize){	
			secondLastPage1++;
		}
		if(streamRandomValue>oneByFiveM-3*pageSize && streamRandomValue<=oneByFiveM-2*pageSize){
			thirdLastPage1++;
		}
		if(streamRandomValue>oneByFiveM-4*pageSize && streamRandomValue<=oneByFiveM-3*pageSize){
			fourthLastPage1++;
		}
		if(streamRandomValue>oneByFiveM-5*pageSize && streamRandomValue<=oneByFiveM-4*pageSize){
			fifthLastPage1++;
		}
		if(streamRandomValue>oneByFiveM-20*pageSize && streamRandomValue<=oneByFiveM){
			lastTwentyPages1++;
		}
		
		//R=1 Million
		if(streamRandomValue>oneM-pageSize && streamRandomValue<=oneM){
			lastPage2++;
		}
		if(streamRandomValue>oneM-2*pageSize && streamRandomValue<=oneM-pageSize){
			secondLastPage2++;
		}
		if(streamRandomValue>oneM-3*pageSize && streamRandomValue<=oneM-2*pageSize){
			thirdLastPage2++;
		}
		if(streamRandomValue>oneM-4*pageSize && streamRandomValue<=oneM-3*pageSize){
			fourthLastPage2++;
		}
		if(streamRandomValue>oneM-5*pageSize && streamRandomValue<=oneM-4*pageSize){
			fifthLastPage2++;
		}
		if(streamRandomValue>oneM-20*pageSize && streamRandomValue<=oneM){
			lastTwentyPages2++;
		}
		
		//R=2 Million
		if(streamRandomValue>twoM-pageSize && streamRandomValue<=twoM){
			lastPage3++;
		}
		if(streamRandomValue>twoM-2*pageSize && streamRandomValue<=twoM-pageSize){
			secondLastPage3++;
		}
		if(streamRandomValue>twoM-3*pageSize && streamRandomValue<=twoM-2*pageSize){
			thirdLastPage3++;
		}
		if(streamRandomValue>twoM-4*pageSize && streamRandomValue<=twoM-3*pageSize){
			fourthLastPage3++;
		}
		if(streamRandomValue>twoM-5*pageSize && streamRandomValue<=twoM-4*pageSize){
			fifthLastPage3++;
		}
		if(streamRandomValue>twoM-20*pageSize && streamRandomValue<=twoM){
			lastTwentyPages3++;
		}
		
		//R=4 Million
		if(streamRandomValue>fourM-pageSize && streamRandomValue<=fourM){
			lastPage4++;
		}
		if(streamRandomValue>fourM-2*pageSize && streamRandomValue<=fourM-pageSize){
			secondLastPage4++;
		}
		if(streamRandomValue>fourM-3*pageSize && streamRandomValue<=fourM-2*pageSize){
			thirdLastPage4++;
		}
		if(streamRandomValue>fourM-4*pageSize && streamRandomValue<=fourM-3*pageSize){
			fourthLastPage4++;
		}
		if(streamRandomValue>fourM-5*pageSize && streamRandomValue<=fourM-4*pageSize){
			fifthLastPage4++;
		}
		if(streamRandomValue>fourM-20*pageSize &&streamRandomValue<=fourM){
			lastTwentyPages4++;
		}
		
		//R=8 Million
		if(streamRandomValue>eightM-pageSize && streamRandomValue<=eightM){
			lastPage5++;
		}
		if(streamRandomValue>eightM-2*pageSize && streamRandomValue<=eightM-pageSize){
			secondLastPage5++;
		}
		if(streamRandomValue>eightM-3*pageSize && streamRandomValue<=eightM-2*pageSize){
			thirdLastPage5++;
		}
		if(streamRandomValue>eightM-4*pageSize && streamRandomValue<=eightM-3*pageSize){
			fourthLastPage5++;
		}
		if(streamRandomValue>eightM-5*pageSize && streamRandomValue<=eightM-4*pageSize){
			fifthLastPage5++;
		}
		if(streamRandomValue>eightM-20*pageSize && streamRandomValue<=eightM){
			lastTwentyPages5++;
		}
		
	}
	
	public int[][] getFrequency(){
		frequency[0][0]=lastPage1;  frequency[0][1]=secondLastPage1;   frequency[0][2]=thirdLastPage1;   frequency[0][3]=fourthLastPage1;   frequency[0][4]=fifthLastPage1;   frequency[0][5]=lastTwentyPages1;
		frequency[1][0]=lastPage2;  frequency[1][1]=secondLastPage2;   frequency[1][2]=thirdLastPage2;   frequency[1][3]=fourthLastPage2;   frequency[1][4]=fifthLastPage2;   frequency[1][5]=lastTwentyPages2;
		frequency[2][0]=lastPage3;  frequency[2][1]=secondLastPage3;   frequency[2][2]=thirdLastPage3;   frequency[2][3]=fourthLastPage3;   frequency[2][4]=fifthLastPage3;   frequency[2][5]=lastTwentyPages3;
		frequency[3][0]=lastPage4;  frequency[3][1]=secondLastPage4;   frequency[3][2]=thirdLastPage4;   frequency[3][3]=fourthLastPage4;   frequency[3][4]=fifthLastPage4;   frequency[3][5]=lastTwentyPages4;
		frequency[4][0]=lastPage5;  frequency[4][1]=secondLastPage5;   frequency[4][2]=thirdLastPage5;   frequency[4][3]=fourthLastPage5;   frequency[4][4]=fifthLastPage5;   frequency[4][5]=lastTwentyPages5;
		
		return frequency;
	}
	
	
}
