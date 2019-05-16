package templates_and_tests;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.execution.columnar.LONG;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class SecondTemplate {

    public static void waitabit(){
        System.out.println("press enter to finish the program");
        try{
            System.in.read();
        }catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args){

        if(args.length == 0){
            throw new IllegalArgumentException("expeting the file name");
        }

        SparkConf conf=new SparkConf(true)
                .setAppName("templates_and_tests.SecondTemplate")
                .setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> docs = sc.textFile(args[0]).cache();
        docs.count();
        long start = System.currentTimeMillis();
        /* Measuring time of this code  */

        /*
        JavaPairRDD<String, Long> wordcountpairs =
                docs.mapToPair(templates_and_tests.SecondTemplate::templates_and_tests.WordCountTest).groupByKey().mapValues(templates_and_tests.SecondTemplate::ReduceFunction);
         */

        JavaPairRDD<String, Long> wordcountpairs = docs
                .repartition(4)
                // Map phase
                .flatMapToPair((document) -> {
                    String[] tokens = document.split(" ");
                    HashMap<String, Long> counts = new HashMap<>();
                    ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
                    for (String token : tokens) {
                        counts.put(token, 1L + counts.getOrDefault(token, 0L));
                    }
                    for (Map.Entry<String, Long> e : counts.entrySet()) {
                        pairs.add(new Tuple2<>(e.getKey(), e.getValue()));
                    }
                    return pairs.iterator();
                })
                // Reduce phase
                .groupByKey()
                .mapValues((it) -> {
                    long sum = 0;
                    for (long c : it) {
                        sum += c;
                    }
                    return sum;
                });

        wordcountpairs.count();
        waitabit();
        /* Measuring time of the code above */
        long end = System.currentTimeMillis();
        System.out.println("Elapsed time: "+(end-start)+" ms.");

    }

    /*
    private static <U> U ReduceFunction(Iterable<Object> objects) {
    }
     */


    /*
    private static <String, Long> Tuple2<String, Long> templates_and_tests.WordCountTest(String document) {
        String[] tokens = (String[]) document.toString().split(" ");
        HashMap<String, Long> counts = new HashMap<>();
        ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
        for (String token : tokens) {
            counts.put(token, 1L + counts.getOrDefault(token, 0L));
        }
        for (Map.Entry<String, Long> e : counts.entrySet()) {
            pairs.add(new Tuple2<>(e.getKey(), e.getValue()));
        }
        return (Tuple2<String, Long>) pairs.iterator();
    }
     */
}

