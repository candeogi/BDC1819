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
 * @author Giovanni Candeo
 */
public class WordCount2_1_2 {

    private static long docWordOccurrences = 3503570;
    private static int k = 0;

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
        collection.repartition(k);

        //lets start measuring time from here
        long start = System.currentTimeMillis();

        List<String> collectcollection = collection.collect();

        JavaPairRDD<String, Long> singleWordsRDD = collection.flatMapToPair(WordCount2_1_2::countSingleWordsFromString);
        JavaPairRDD<Integer, Iterable<Tuple2<String, Long>>> subsetByKey = singleWordsRDD.groupBy(WordCount2_1_2::assignRandomKey);
        //fine map phase 1
        List<Tuple2<Integer, Iterable<Tuple2<String, Long>>>> collect = subsetByKey.collect();
        waitabit();

        //end of time measuring
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
    

    private static int assignRandomKey(Tuple2<String, Long> stringLongTuple2) {
        return ThreadLocalRandom.current().nextInt(0, (int) Math.sqrt(k));
    }

    private static Iterator<Tuple2<String,Long>> countSingleWordsFromString(String documentsPartition) {
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

    private static Iterator<Tuple2<String,Long>> countSingleWords(List<String> documentsPartition) {
        HashMap<String,Long> counts = new HashMap<>();
        ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
        for(String partitionDoc : documentsPartition){
            for(String token : partitionDoc.split(" ")) {
                counts.put(token, 1L + counts.getOrDefault(token, 0L));
            }
        }
        for(Map.Entry<String,Long> e: counts.entrySet()){
            pairs.add(new Tuple2<>(e.getKey(),e.getValue()));
        }
        return pairs.iterator();
    }
}

