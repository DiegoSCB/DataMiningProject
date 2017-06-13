package bigdata;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

import bigdata.EventosFranceses.Day;
import bigdata.EventosFranceses.Filter;
import bigdata.EventosFranceses.Filter2;
import bigdata.EventosFranceses.Week;

public class EvolutionEvents {
	public static void main(String[] args) throws Exception {
        
        // set up the execution environment
 
	 	final ParameterTool params = ParameterTool.fromArgs(args);
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);
         
        
        // get input data
       
       // DataSet<String>   text = env.readTextFile(params.get("input"));
       String wantedfields = "0100100000000000000000000000100000001000000100000010000000";
       //CsvReader text = env.readCsvFile(params.get("input"));
       String delimited = "\t";
       String line = "\n";
       DataSet < Tuple6< String, String, String, String,String,String>> tuplas = env.readCsvFile(params.get("input"))
    		   .fieldDelimiter(delimited)
    		   .includeFields(wantedfields)
    		   .types(String.class, String.class, String.class, String.class,String.class,String.class);
       
       
      /* DataSet< Tuple3< String, String, Integer>> perday = tuplas.map(new Week())
       .groupBy(0, 1)
       .sum(3)
       .first(5)
       .print();*/
       DataSet<Tuple5<String,String, String,String, Integer>> perweek = tuplas
    		   .filter(new Filter2())
    		   .flatMap(new Week())
    		   .filter(new Filter())
    		   .groupBy(0,1,2)
       		   .sum(4)
       		   .sortPartition(4, Order.DESCENDING)
       		   .first(500);
       DataSet<Tuple5<String,String, String,String, Integer>> perday = tuplas
    		   .filter(new Filter2())
    		   .flatMap(new Day())
    		   .filter(new Filter())
    		   .groupBy(0,1,2)
       		   .sum(4)
       		   .sortPartition(4, Order.DESCENDING)
       		   .first(500);
       
       perweek.writeAsCsv(params.get("output2"), line, delimited);
       perday.writeAsCsv(params.get("output1"), line, delimited);
       env.execute("Top5countries per week and day");
      }
 public static class Week implements FlatMapFunction< Tuple6 < String,String,String,String,String,String >, Tuple5<String,String,String,String,Integer>> {
      
	private static final long serialVersionUID = 1L;

		public void flatMap(Tuple6 < String, String, String, String,String,String> original, Collector<Tuple5<String,String,String,String,Integer>> out) throws Exception {
			  
              String ntimes ="nº of times ";
			  String percentageweek = original.f1;
              String dateTokens[] = percentageweek.split("\\.");
              double week = Double.parseDouble("0."+ dateTokens[1]);
              //System.out.println("AAAAAAAAAAAAAAAA"+ week +"\\\\\\\\\\\\\\\\\\\\\\");
              week = week *52.0;
              int auxweek = (int)week;
              String weekandyear = "year: "+dateTokens[0]+" week:"+String.valueOf(auxweek);
              String totalgeo1 = original.f3;
              String totalgeo2 = original.f4;
              String totalgeo3 = original.f5;
              
              String countryTokens1[] = totalgeo1.split(",");
              String countryTokens2[] = totalgeo2.split(",");
              String countryTokens3[] = totalgeo3.split(",");
              String country1 = countryTokens1[countryTokens1.length -1];
              String country2 = countryTokens2[countryTokens2.length -1];
              String country3 = countryTokens3[countryTokens3.length -1];
              //String country = "empty";
              /*if(original.f2 != null){
            	  String separatedcountry[] = original.f2.split(",");
            	  country = separatedcountry[separatedcountry.length -1];
              }*/
             
            out.collect(new Tuple5<>(weekandyear, country1,original.f2,ntimes, 1));
            out.collect(new Tuple5<>(weekandyear, country2,original.f2,ntimes, 1));
            out.collect(new Tuple5<>(weekandyear, country3,original.f2,ntimes, 1));
            
        }
    }
 public static class Day implements FlatMapFunction< Tuple6 < String,String,String,String,String,String >, Tuple5<String,String,String,String,Integer>> {
     
	private static final long serialVersionUID = 1L;

	public void flatMap(Tuple6 < String, String, String, String,String,String> original, Collector<Tuple5<String,String,String,String,Integer>> out) throws Exception {
		  
              String ntimes ="nº of times ";
              char date[] = DateExtractor(original.f0);
              //String weekwithoutyear = "year: "+dateTokens[0]+" week:"+String.valueOf(auxweek);
              String date2[] = original.f1.split("\\.");
              String year = date2[0];
              String day = date[date.length-2]+""+date[date.length-1]+"/"+date[date.length-4]+""+date[date.length-3]+"/"+year;
             /* String country = "empty";
              if(original.f2 != null){
            	  String separatedcountry[] = original.f2.split(",");
            	  country = separatedcountry[separatedcountry.length -1];
              }*/
              String totalgeo1 = original.f3;
              String totalgeo2 = original.f4;
              String totalgeo3 = original.f5;
              
              String countryTokens1[] = totalgeo1.split(",");
              String countryTokens2[] = totalgeo2.split(",");
              String countryTokens3[] = totalgeo3.split(",");
              String country1 = countryTokens1[countryTokens1.length -1];
              String country2 = countryTokens2[countryTokens2.length -1];
              String country3 = countryTokens3[countryTokens3.length -1];
             
                out.collect(new Tuple5<>(day, country1 ,original.f2, ntimes, 1));
                out.collect(new Tuple5<>(day, country2 ,original.f2, ntimes, 1));
                out.collect(new Tuple5<>(day, country3 ,original.f2, ntimes, 1));
        }
		public static char[] DateExtractor(String string){
			char[] aux = new char[string.length()];
			for (int i = 0; i < string.length(); i++){
			    aux[i] = string.charAt(i);
		}
			return aux;
		}
 }
		 public static final class Filter implements FilterFunction<Tuple5<String, String, String,String, Integer>> {
		 		
			 
				private static final long serialVersionUID = 1L;
					//private String[] countries = {"FRA","GUF","PYF"};
					@Override
					public boolean filter(Tuple5<String, String, String,String,Integer> original) {
						
							if(original.f1.equals("France")){
								return true;
							}
						
						return false;
					}
		}
		 public static final class Filter2 implements FilterFunction<Tuple6<String, String, String, String,String,String>> {
		 		
			 
				private static final long serialVersionUID = 1L;
					
					@Override
					public boolean filter(Tuple6<String, String, String,String,String,String> original) {
						  String percentageweek = original.f1;
			              String dateTokens2[] = percentageweek.split("\\.");
			              if(!dateTokens2[0].equals("2017")){
			            	  return false;
			              }
			              return true;
					}
		}
 }



