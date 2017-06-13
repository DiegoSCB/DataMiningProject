package bigdata;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

import bigdata.TrendingLeaders.Day;
import bigdata.TrendingLeaders.Ratio;
import bigdata.TrendingLeaders.Reduce;
import bigdata.TrendingLeaders.Transformation;
import bigdata.TrendingLeaders.Week;

public class TrendingLeadersFRA {
	public static void main(String[] args) throws Exception {
        
        // set up the execution environment
 
	 	final ParameterTool params = ParameterTool.fromArgs(args);
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);
         
        
        // get input data
       
       // DataSet<String>   text = env.readTextFile(params.get("input"));
       String wantedfields = "0100101000000000100000000000000100001000000100000010000000";
       //CsvReader text = env.readCsvFile(params.get("input"));
       String delimited = "\t";
       String line = "\n";
       DataSet < Tuple8< String, String, String,String,Integer,String,String,String>> tuplas = env.readCsvFile(params.get("input"))
    		   .fieldDelimiter(delimited)
    		   .includeFields(wantedfields)
    		   .types(String.class, String.class, String.class,String.class,Integer.class,String.class,String.class,String.class);
       
       
      /* DataSet< Tuple3< String, String, Integer>> perday = tuplas.map(new Week())
       .groupBy(0, 1)
       .sum(3)
       .first(5)
       .print();*/
       DataSet<Tuple5<String,String, Integer,Integer,Double>> perweek = tuplas
    		   .flatMap(new Week())
    		   .filter(new Filter())
    		   .groupBy(0,1)
    		   .sum(2)
       		   .map(new Transformation())
       		   .groupBy(0,1)
       		   .reduce(new Reduce())
       		   .map(new Ratio())
       		   //.filter(new Filter2())
       		   .sortPartition(2, Order.DESCENDING)
       		   //.sortPartition(1, Order.DESCENDING)
       		   .first(1000);
       		   
       		   
       DataSet<Tuple5<String,String, Integer,Integer,Double>> perday = tuplas
    		   .flatMap(new Day())
    		   .filter(new Filter())
    		   .groupBy(0,1)
    		   .sum(2)
       		   .map(new Transformation())
       		   .groupBy(0,1)
       		   .reduce(new Reduce())
       		   .map(new Ratio())
       		   //.filter(new Filter2())
       		   .sortPartition(2, Order.DESCENDING)
       		   //.sortPartition(1, Order.DESCENDING) 
       		   .first(1000);
       
        perday.writeAsCsv(params.get("output1"), line, delimited);
        perweek.writeAsCsv(params.get("output2"), line, delimited);
        
       env.execute("Trending leaders");
       
      }
 public static class Week implements FlatMapFunction< Tuple8 < String,String,String,String,Integer,String,String,String >, Tuple4<String,String,Integer,String>> {
      
	private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(Tuple8 < String, String, String, String, Integer,String,String,String > original, Collector<Tuple4<String, String, Integer,String>> output) throws Exception {
			  
			  String percentageweek = original.f1;
              String dateTokens[] = percentageweek.split("\\.");
              double week = Double.parseDouble("0."+ dateTokens[1]);
              
              week = week *52.0;
              int auxweek = (int)week;
              String weekandyear = "year: "+dateTokens[0]+" week:"+String.valueOf(auxweek);
              String totalgeo1 = original.f5;
              String totalgeo2 = original.f6;
              String totalgeo3 = original.f7;
              
              String countryTokens1[] = totalgeo1.split(",");
              String countryTokens2[] = totalgeo2.split(",");
              String countryTokens3[] = totalgeo3.split(",");
              String country1 = countryTokens1[countryTokens1.length -1];
              String country2 = countryTokens2[countryTokens2.length -1];
              String country3 = countryTokens3[countryTokens3.length -1];
              
              output.collect(new Tuple4<>(weekandyear, original.f2, original.f4,country1));
	          output.collect(new Tuple4<>(weekandyear, original.f3, original.f4,country2));
           
           
        }
    }
 public static class Day implements FlatMapFunction< Tuple8 < String,String,String,String,Integer,String,String,String >, Tuple4<String,String,Integer,String>> {
      
	private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(Tuple8 < String, String, String, String, Integer,String,String,String > original, Collector<Tuple4<String, String, Integer,String>> output) throws Exception {
			  
              char date[] = DateExtractor(original.f0);
              //String weekwithoutyear = "year: "+dateTokens[0]+" week:"+String.valueOf(auxweek);
              String date2[] = original.f1.split("\\.");
              String year = date2[0];
              String day = date[date.length-2]+""+date[date.length-1]+"/"+date[date.length-4]+""+date[date.length-3]+"/"+year;
              String totalgeo1 = original.f5;
              String totalgeo2 = original.f6;
              String totalgeo3 = original.f7;
              
              String countryTokens1[] = totalgeo1.split(",");
              String countryTokens2[] = totalgeo2.split(",");
              String countryTokens3[] = totalgeo3.split(",");
              String country1 = countryTokens1[countryTokens1.length -1];
              String country2 = countryTokens2[countryTokens2.length -1];
              String country3 = countryTokens3[countryTokens3.length -1];
              
              output.collect(new Tuple4<>(day, original.f2, original.f4,country1));
	          output.collect(new Tuple4<>(day, original.f3, original.f4,country2));
              
            
        }
		public static char[] DateExtractor(String string){
			char[] aux = new char[string.length()];
			for (int i = 0; i < string.length(); i++){
			    aux[i] = string.charAt(i);
		}
			return aux;
		}
		
 }
 public static final class Reduce implements ReduceFunction<Tuple4<String,String,Integer,Integer>> {
 		
 
	private static final long serialVersionUID = 1L;
		
		@Override
		public Tuple4<String,String,Integer,Integer> reduce(Tuple4<String,String, Integer,Integer> original, 
				Tuple4<String,String,Integer,Integer> secondoriginal) throws Exception {
			return new Tuple4<>(original.f0,original.f1,original.f2+secondoriginal.f2,original.f3 + secondoriginal.f3);
		}
}
public static class Transformation implements MapFunction <Tuple4 <String, String, Integer,String>, Tuple4 <String,String,Integer,Integer>>{
	 
private static final long serialVersionUID = 1L;

	@Override
	 public Tuple4<String,String,Integer,Integer> map (Tuple4 <String, String, Integer,String> original){
		 
		 return new Tuple4<>(original.f0,original.f1,original.f2,1);
	 }
		 
	 }
public static class Ratio implements MapFunction <Tuple4 <String,String, Integer, Integer>, Tuple5 <String,String, Integer,Integer,Double>>{
 


	@Override
	 public Tuple5<String,String, Integer,Integer,Double> map (Tuple4 <String,String, Integer, Integer> original2){
		 double ratio  = (original2.f2/original2.f3);
		 ratio = Math.round(ratio*100d/100d);
		 return new Tuple5<>(original2.f0, original2.f1, original2.f2,original2.f3,ratio);
	 }
		 
	 }
public static final class Filter implements FilterFunction<Tuple4<String, String, Integer,String>> {
		
	 
	private static final long serialVersionUID = 1L;
		//private String[] countries = {"FRA","GUF","PYF"};
		@Override
		public boolean filter(Tuple4<String, String,Integer,String> original) {
			
				if(original.f3.equals("France")){
					return true;
				}
			
			return false;
		}
}
public static final class Filter2 implements FilterFunction<Tuple5<String, String, Integer, Integer,Double>> {
		
	 
	private static final long serialVersionUID = 1L;
		
		@Override
		public boolean filter(Tuple5<String, String, Integer, Integer,Double> original) {
			  String percentageweek = original.f0;
              String dateTokens2[] = percentageweek.split("\\.");
              if(!dateTokens2[0].equals("2017")){
            	  return false;
              }
              return true;
		}
}
}
