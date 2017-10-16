import scala.Tuple2;

import com.google.common.collect.Iterables;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Iterator;
import java.util.regex.Pattern;

class sortby implements Comparator<Tuple2<String, Double>>{

    @Override
    public int compare(Tuple2<String, Double> o1, Tuple2<String, Double> o2) {
        // TODO Auto-generated method stub
       if( o1._2 <  o2._2)  return 1;
       else if(o1._2 > o2._2) return -1;
       else return 0;
        
    }
}

public final class PageRank {
  private static final Pattern TAB = Pattern.compile("\\t");

  @SuppressWarnings("serial")
  private static class Sum implements Function2<Double, Double, Double> {
    @Override
    public Double call(Double a, Double b) {
      return a + b;
    }
  }

  @SuppressWarnings("serial")
public static void main(String[] args) throws Exception {
    if (args.length < 4) {
      System.err.println("Specify arguments master, text file , iterations , 0 for all  or 1 for universities ");
      System.exit(1);
    }
    JavaSparkContext ctx = new JavaSparkContext(args[0], "PageRank",
      System.getenv("SPARK_HOME"), JavaSparkContext.jarOfClass(PageRank.class));
    JavaRDD<String> lines = ctx.textFile(args[1], 1);
    JavaPairRDD<String, Iterable<String>> links = lines.mapToPair(new PairFunction<String, String, String>() {
      @Override
      public Tuple2<String, String> call(String s) {
        String[] parts = TAB.split(s);
        return new Tuple2<String, String>(parts[0], parts[1]);
      }
    }).distinct().groupByKey().cache();

    
    JavaPairRDD<String, Double> ranks = links.mapValues(new Function<Iterable<String>, Double>() {
      @Override
      public Double call(Iterable<String> rs) {
        return 1.0;
      }
    });

    for (int current = 0; current < Integer.parseInt(args[2]); current++) {
      JavaPairRDD<String, Double> contribs = links.join(ranks).values()
        .flatMapToPair(new PairFlatMapFunction<Tuple2<Iterable<String>, Double>, String, Double>() {
          @Override
          public Iterator<Tuple2<String, Double>> call(Tuple2<Iterable<String>, Double> s) {
              
             int urlCount = Iterables.size(s._1);
             List<Tuple2<String, Double>> results = new ArrayList<Tuple2<String, Double>>();
             for (String n : s._1) {
                (results).add(new Tuple2<String, Double>(n, s._2() / urlCount));
             }
             return results.iterator();
           }
      });
      ranks = contribs.reduceByKey(new Sum()).mapValues(new Function<Double, Double>() {
        @Override
        public Double call(Double sum) {
          return 0.15 + sum * 0.85;
        }
      });
    }
    List<Tuple2<String, Double>> output = ranks.collect();
    int i = 1;
    List<Tuple2<String, Double>> sortedop = new ArrayList<Tuple2<String,Double>>(output);
    Collections.sort(sortedop, new sortby());
    if(args[3].equals ("0")){
        for (Tuple2<?,?> tuple :sortedop) {
            System.out.println(i++ +")"+ tuple._1());
            if(i == 101) break;
        } 
    }
    else {
        for (Tuple2<?,?> tuple :sortedop) {
            //String s = "sainath";
            
            if(tuple._1().toString().matches(".*(niversity|nstitute|Institution).*"))
                System.out.println(i++ + ")"+tuple._1());
            if( i == 101) break;
            
        } 
        
    }
    
    ctx.stop();
  }
}