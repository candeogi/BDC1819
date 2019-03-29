
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

public class WordCountTest {
    public static void main(String[] args){

        if(args.length == 0){
            throw new IllegalArgumentException("expeting the file name");
        }

        SparkConf conf=new SparkConf(true)
                .setAppName("WordCountTest")
                .setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        //the test-sample file contains documents composed by a single line of words.
        // #docs 10122
        // #words 3503570

        //RDD contains 10122 docs composed by a single line of numerous strings
        JavaRDD<String> document = sc.textFile(args[0]);
        System.out.println("TEST COUNT: "+document.count());

        JavaRDD<String, Long> countForSingleWord = document.flatMapToPair(WordCountTest::countSingleWords);


    }

    private static <K2, V2> Iterator<Tuple2<String,Long>> countSingleWords(String s) {
        String[] words = s.split(" ");
        ArrayList<Tuple2<String, Long>> counts = new ArrayList<>();
        for(String w : words){
            counts.append(new Tuple2<>(w,1));
        }
        return counts.iterator();
    }
}

