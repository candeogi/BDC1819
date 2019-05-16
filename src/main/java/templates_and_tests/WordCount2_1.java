package templates_and_tests;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * WordCount2
 * version 1
 * flatmaptopair -> grouby -> flatmaptopair -> reducebykey
 * @author Giovanni Candeo
 */
public class WordCount2_1 {

    private static long docWordOccurrences = 3503570;
    private static int k = 0;

    private static void waitabit(){
        System.out.println("press enter to finish the program");
        try{
            System.in.read();
        }catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args){

        if(args.length == 0){
            throw new IllegalArgumentException("expecting the file name");
        }

        SparkConf conf=new SparkConf(true)
                .setAppName("WordCount2")
                .setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        //the test-sample file contains documents composed by a single line of words.
        // #docs 10122
        // #words 3503570

        //RDD contains 10122 docs composed by a single line of numerous strings
        JavaRDD<String> collection = sc.textFile(args[0]).cache();
        System.out.println("Test count: "+collection.count());

        //number of partitions K, received as an input in the command line
        k = Integer.parseInt(args[1]);

        //-------------TIME MEASURE START ------------------
        long start = System.currentTimeMillis();

        JavaPairRDD<String, Long> dWordCount2Pairs = collection
                .repartition(k)
                .flatMapToPair(WordCount2_1::countSingleWordsFromDocString)
                .groupBy(WordCount2_1::assignRandomKey)
                .flatMapToPair(WordCount2_1::sumWOccurrencesOfKSubsets)
                .reduceByKey(Long::sum);

        dWordCount2Pairs.cache();
        dWordCount2Pairs.count();

        //waitabit();

        //-------------TIME MEASURE END --------------------
        long end = System.currentTimeMillis();
        System.out.println("Elapsed time: " + (end - start) + " ms");

        /*
        //print the word count just to visualize
        //steel must be 127
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

    private static Iterator<Tuple2<String,Long>> countSingleWordsFromDocString(String documentsPartition) {
        String[] tokens = documentsPartition.split(" ");
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

    private static int assignRandomKey(Tuple2<String, Long> stringLongTuple2) {
        return ThreadLocalRandom.current().nextInt(0, (int) Math.sqrt(k));
    }

    private static Iterator<Tuple2<String,Long>> sumWOccurrencesOfKSubsets(Tuple2<Integer, Iterable<Tuple2<String, Long>>> keyWordCountPairs) {
        ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
        HashMap<String,Long> sumOfWordCount = new HashMap<>();
        for(Tuple2<String,Long> wcPair : keyWordCountPairs._2){
            sumOfWordCount.put(wcPair._1(), sumOfWordCount.getOrDefault(wcPair._1(), 0L)+wcPair._2());
        }
        for (Map.Entry<String, Long> e : sumOfWordCount.entrySet()) {
            pairs.add(new Tuple2<>(e.getKey(), e.getValue()));
        }
        return pairs.iterator();
    }

}

