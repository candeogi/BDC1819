import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Group 32
 * File for the Homework n.2 of "Big Data Computing" Course"
 *
 * @author Giovanni Candeo 1206150
 * @author Nicolo Levorato 1156744
 *
 */
public class G32HM2 {

    //number of partitions
    private static int k = 0;

    /**
     * Main method
     * @param args args[0] contains the name of the file txt we want to count
     *             args[1] contains the number of k partitions
     */
    public static void main(String[] args) {

        if (args.length == 0) {
            throw new IllegalArgumentException("expecting the file name");
        }

        SparkConf conf = new SparkConf(true)
                .setAppName("G32HM2")
                .setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        //RDD contains 10122 docs composed by a single line of numerous strings
        JavaRDD<String> collection = sc.textFile(args[0]).cache();
        System.out.println("Test count: " + collection.count());

        //number of partitions K, received as an input in the command line
        k = Integer.parseInt(args[1]);

        ArrayList<Long> speedTest = new ArrayList<>();
        long start;
        long end;

        /*---------Word Count 1---------*/
        start = System.currentTimeMillis();

        JavaPairRDD<String,Long> dWordCount1Pairs =collection
                .repartition(k)
                .flatMapToPair(G32HM2::countSingleWordsFromString)
                .reduceByKey(Long::sum);

        //i need this for computing the actual RDD transformation
        dWordCount1Pairs.cache();
        dWordCount1Pairs.count();

        //waitHere();
        end = System.currentTimeMillis();
        speedTest.add(end-start);

        /*---------Word Count 2.1-------*/
        collection = sc.textFile(args[0]).cache();
        collection.count();

        start = System.currentTimeMillis();

        JavaPairRDD<String, Long> dWordCount2Pairs = collection
                .repartition(k)
                .groupBy(G32HM2::assignRandomKey)
                .flatMapToPair(G32HM2::wordCountInPartition1)
                .reduceByKey(Long::sum);

        dWordCount2Pairs.cache();
        dWordCount2Pairs.count();
        //waitHere();

        end = System.currentTimeMillis();
        speedTest.add(end-start);

        /*---------Word Count 2.2-------*/
        collection = sc.textFile(args[0]).cache();
        collection.count();

        start = System.currentTimeMillis();

        JavaPairRDD<String, Long> dWordCount2Pairs2 =  collection
                .repartition(k)
                .mapPartitionsToPair(G32HM2::wordCountInPartition2)
                .reduceByKey(Long::sum);

        dWordCount2Pairs2.cache();
        dWordCount2Pairs2.count();
        //waitHere();

        end = System.currentTimeMillis();
        speedTest.add(end-start);

        /*-------------------------------*/

        System.out.println("" +
                "@@@@ TIME @@@ SPEED @@@ TEST @@@@@@\n" +
                "Improved count 1: "+speedTest.get(0)+" ms\n" +
                "Improved count 2.1: "+speedTest.get(1)+" ms\n" +
                "Improved count 2.2: "+speedTest.get(2)+" ms\n" +
                "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");

        /*
        //We can print also with - WATCH OUT: output are big files!
        printWordCount(dWordCount1Pairs,"wc1output.txt");
        printWordCount(dWordCount2Pairs,"wc2_1output.txt");
        printWordCount(dWordCount2Pairs2,"wc2_2output.txt");
        */
    }

    /**
     * Count the word occurrences in a document represented by a single line of strings.
     *
     * @param document input document as a single string line.
     * @return iterator of tuple composed by word and his count.
     */
    private static Iterator<Tuple2<String,Long>> countSingleWordsFromString(String document) {
        String[] tokens = document.split(" ");
        HashMap<String, Long> wordCountInDocument = new HashMap<>();
        ArrayList<Tuple2<String, Long>> wcPairs = new ArrayList<>();
        for (String token : tokens) {
            wordCountInDocument.put(token, 1L + wordCountInDocument .getOrDefault(token, 0L));
        }
        for (Map.Entry<String, Long> e : wordCountInDocument .entrySet()) {
            wcPairs.add(new Tuple2<>(e.getKey(), e.getValue()));
        }
        return wcPairs.iterator();
    }

    /**
     * Assigns a random key to the input document s
     * @param s input document s
     * @return random Long value that will represent a random key.
     */
    private static Long assignRandomKey(String s) {
        return ThreadLocalRandom.current().nextLong(0, (int) Math.sqrt(k));
    }

    /**
     * This function is applied only to the single partitions.
     * Counts and sums the occurrences of words within a partition of documents.
     * Basically implements the first round of Word Count 2
     *
     * @param partition contains(x,[W1,W2,...,Wk])
     *                  where x is a random key
     *                  [W1,W2 .. ] is the partition of docs with assigned key
     * @return returns new pairs where the word w is the key and count(w) the value.
     */
    private static Iterator<Tuple2<String, Long>> wordCountInPartition1(Tuple2<Long, Iterable<String>> partition) {
        ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
        HashMap<String, Long> counts = new HashMap<>();
        //per ogni documento conto le parole
        for(String document: partition._2()){
            String[] tokens = document.split(" ");
            for (String token : tokens) {
                counts.put(token, 1L + counts.getOrDefault(token, 0L));
            }
        }
        for (Map.Entry<String, Long> e : counts.entrySet()) {
            pairs.add(new Tuple2<>(e.getKey(), e.getValue()));
        }
        return pairs.iterator();
    }

    /**
     * This function operate on single partitions
     *
     * @param documentsPartitioned single partition of documents.
     * @return word count occurrences in the partition
     **/
    private static Iterator<Tuple2<String,Long>> wordCountInPartition2(Iterator<String> documentsPartitioned) {
        HashMap<String,Long> wordCountInPartition = new HashMap<>();

        //i need this to return a correct iterator
        ArrayList<Tuple2<String,Long>> wordCountIterator = new ArrayList<>();

        //per ogni documento della partizione conto le occorrenze di parole e aggiorno a fine ciclo il conteggio principale
        while(documentsPartitioned.hasNext()){
            HashMap<String,Long> wordCountInDocument = new HashMap<>();
            String[] tokens = documentsPartitioned.next().split(" ");
            //count word occurrences in document
            for(String token : tokens){
                wordCountInDocument.merge(token,1L,Long::sum);
            }
            //document has been wordcounted, time to update the main value
            for (Map.Entry<String, Long> e : wordCountInDocument.entrySet()) {
                wordCountInPartition.merge(e.getKey(),e.getValue(),Long::sum);
            }
        }
        for (Map.Entry<String, Long> e : wordCountInPartition.entrySet()) {
            wordCountIterator.add(new Tuple2<>(e.getKey(), e.getValue()));
        }
        return wordCountIterator.iterator();
    }

    /**
     * Utility function
     * Prints the RDD to visualize the word count pairs in a txt file.
     *
     * @param dWordCountPairsRDD the RDD that is printed to file
     * @param filename the output file of the print
     */
    private static void printWordCount(JavaPairRDD<String,Long> dWordCountPairsRDD, String filename){
        Map<String, Long> lWordCountPairs = dWordCountPairsRDD.collectAsMap();
        FileWriter fileWriter = null;
        try {
            fileWriter = new FileWriter(filename);
        } catch (IOException e) {
            e.printStackTrace();
        }
        PrintWriter printWriter = new PrintWriter(fileWriter);
        printWriter.print(lWordCountPairs.toString());
        printWriter.close();
    }

    /**
     * Utility function
     * Stops the execution of the program to interact with the Spark webapp.
     */
    private static void waitHere(){
        System.out.println("press enter to finish the program");
        try{
            System.in.read();
        }catch (IOException e) {
            e.printStackTrace();
        }
    }
}
