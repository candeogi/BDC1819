package templates_and_tests;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class testclass {
    private static int k=8;
    public static void main(String[] args) {

        SparkConf conf = new SparkConf(true)
                .setAppName("templates_and_tests.testclass")
                .setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> collection = sc.textFile(args[0]);
            /*
        JavaRDD<String> repartition = collection.repartition(k);
        JavaPairRDD<String, Long> stringLongJavaPairRDD = repartition.flatMapToPair(templates_and_tests.testclass::testfunction);
        JavaPairRDD<Long, Iterable<Tuple2<String, Long>>> longIterableJavaPairRDD = stringLongJavaPairRDD.groupBy((w) -> ThreadLocalRandom.current().nextLong(0, (int) Math.sqrt(k)));
        JavaPairRDD<String, Long> asdasdasd = longIterableJavaPairRDD.flatMapToPair(templates_and_tests.testclass::testtest);
        JavaPairRDD<String, Long> stringLongJavaPairRDD1 = asdasdasd.reduceByKey(Long::sum);
*/
        JavaPairRDD<String,Long> finalwordcount = collection.repartition(k)
                                        .flatMapToPair(testclass::testfunction)
                                        .groupBy((w) -> ThreadLocalRandom.current().nextLong(0, (int) Math.sqrt(k)))
                                        .flatMapToPair(testclass::testtest)
                                        .reduceByKey(Long::sum);


    }


    private static Iterator<Tuple2<String,Long>> testtest(Tuple2<Long, Iterable<Tuple2<String, Long>>> subsetbykey) {
        //(3, [("cat",3), ("dog",2), ("cat",4)])
        Iterable<Tuple2<String, Long>> tuple2s = subsetbykey._2();
        HashMap<String,Long> count = new HashMap<>();
        for(Tuple2<String,Long> singolaCoppia : tuple2s){
            count.merge(singolaCoppia._1(),singolaCoppia._2(),Long::sum);
        }
        ArrayList<Tuple2<String,Long>> pairs= new ArrayList<>();
        for(Map.Entry<String, Long> e : count.entrySet()){
            pairs.add(new Tuple2<>(e.getKey(), e.getValue()));
        }
        return pairs.iterator();
    }


    private static Iterator<Tuple2<String, Long>> testfunction(String document) {
        //"ciao asd ciao com"
        String[] documentSplitted= document.split(" ");
        HashMap<String,Long> wc = new HashMap<>();
        for(String parola :  documentSplitted ){
            wc.merge(parola,1L ,Long::sum);
        }
        ArrayList<Tuple2<String,Long>> pairs= new ArrayList<>();
        for(Map.Entry<String, Long> e : wc.entrySet()){
            pairs.add(new Tuple2<>(e.getKey(), e.getValue()));
        }
        return pairs.iterator();
    }
}