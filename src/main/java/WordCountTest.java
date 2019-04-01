
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.util.*;

/**
 * This class is a remake of the SecondTemplate file provided from the homework 2.
 * The class implement a word count algorithm as in the SecondTemplate but uses methods instead of lambda functions.
 * Also makes a time measurement of the time needed for the algorithm to complete.
 * @author Giovanni Candeo
 */
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
        JavaRDD<String> document = sc.textFile(args[0]).cache();
        System.out.println("Test count: "+document.count());

        //lets start measuring time from here
        long start = System.currentTimeMillis();

        //Iterator<Tuple2<String, Long>> countForSingleWord = document.flatMapToPair(WordCountTest::countSingleWords);
        JavaPairRDD<String,Long> dWordCountPairs = document
                .flatMapToPair(WordCountTest::countSingleWords)
                .groupByKey()
                .mapValues(WordCountTest::countOccurrences);

        //end of time measuring
        long end = System.currentTimeMillis();
        System.out.println("Elapsed time: " + (end - start) + " ms");

    }


    private static Iterator<Tuple2<String,Long>> countSingleWords(String s) {
        String[] words = s.split(" ");
        ArrayList<Tuple2<String, Long>> counts = new ArrayList<>();
        for(String w : words){

            counts.add(new Tuple2<>(w,1L));
        }
        return counts.iterator();
    }

    private static Long countOccurrences(Iterable<Long> iterable) {
        Long sum = Long.valueOf(0);
        for(Long c: iterable){
            sum += c;
        }
        return sum;
    }


}

