import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

/**
 * WordCount1
 *
 * @author Giovanni Candeo
 */
public class WordCount1 {
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

        //number of partitions K, received as an input in the command line
        int k = Integer.parseInt(args[1]);


        //lets start measuring time from here
        long start = System.currentTimeMillis();

        //Iterator<Tuple2<String, Long>> countForSingleWord = document.flatMapToPair(WordCountTest::countSingleWords);
        JavaPairRDD<String,Long> dWordCountPairs = document
                .flatMapToPair(WordCount1::countSingleWords)
                .reduceByKey(Long::sum);

        //end of time measuring
        long end = System.currentTimeMillis();
        System.out.println("Elapsed time: " + (end - start) + " ms");

        /*
        //print the word count just to visualize
        Map<String, Long> lWordCountPairs = dWordCountPairs.collectAsMap();
        //System.out.println(lWordCountPairs.toString());
        FileWriter fileWriter = null;
        try {
            fileWriter = new FileWriter("wordcountprintfile.txt");
        } catch (IOException e) {
            e.printStackTrace();
        }
        PrintWriter printWriter = new PrintWriter(fileWriter);
        printWriter.print(lWordCountPairs.toString());
        printWriter.close();
        */
    }


    private static Iterator<Tuple2<String,Long>> countSingleWords(String s) {
        String[] words = s.split(" ");
        ArrayList<Tuple2<String, Long>> counts = new ArrayList<>();
        for(String w : words){

            counts.add(new Tuple2<>(w,1L));
        }
        return counts.iterator();
    }
}

