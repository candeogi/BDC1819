
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class SecondTemplateRemake {
    public static void main(String[] args){

        if(args.length == 0){
            throw new IllegalArgumentException("expeting the file name");
        }

        SparkConf conf=new SparkConf(true)
                .setAppName("SecondTemplate")
                .setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> docs = sc.textFile(args[0]).cache();
        docs.count();
        long start = System.currentTimeMillis();
        /* Measuring time of this code  */

        JavaPairRDD<String, Long> wordcountpairs = docs
                        .flatMapToPair(SecondTemplateRemake::WordCountTest)
                        .groupByKey()
                        .mapValues(SecondTemplateRemake::ReduceFunction);


        /* Measuring time of the code above */
        long end = System.currentTimeMillis();
        System.out.println("Elapsed time: "+(end-start)+" ms.");

        System.out.println("press enter to finish the program");
        try{
            System.in.read();
        }catch (IOException e) {
            e.printStackTrace();
        }

    }

    private static <K2, V2> Iterator<Tuple2<String, Long>> WordCountTest(String document) {
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
    }

    private static Long ReduceFunction(Iterable<Long> iterable) {
        long sum = 0;
        for(long c: iterable){
            sum += c;
        }
        return sum;
    }

}

