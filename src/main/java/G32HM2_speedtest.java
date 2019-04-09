import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Group 32
 * File for the Homework n.2 of "Big Data Computing" Course"
 *
 * This file:
 * 1-Reads the collection of documents into an RDD docs and subdivides the into K parts;
 * 2-Runs three MapReduce Word count algorithms and returns their individual running times, carefully measured.
 * 3-Prints the average length of the distinct words appearing in the collection.
 *
 * @author Giovanni Candeo 1206150
 * @author Nicolo Levorato 1156744
 *
 */
public class G32HM2_speedtest {

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

        //number of partitions K, received as an input in the command line
        k = Integer.parseInt(args[1]);

        //we make k partitions of the initial collection
        JavaRDD<String> collectionRepartitioned = collection.repartition(k);
        collectionRepartitioned.cache().count();

        ArrayList<Long> speedTest = new ArrayList<>();
        long start;
        long end;

        /*---------Word Count 1---------*/
        start = System.currentTimeMillis();

        JavaPairRDD<String,Long> dWordCount1Pairs =collectionRepartitioned
                .flatMapToPair(G32HM2_speedtest::countSingleWordsFromString)
                .reduceByKey(Long::sum);

        //i need this for computing the actual RDD transformation
        dWordCount1Pairs.cache().count();

        //waitHere();
        end = System.currentTimeMillis();
        speedTest.add(end-start);

        /*---------Word Count 2.1-------*/
        start = System.currentTimeMillis();

        JavaPairRDD<String, Long> dWordCount2Pairs =collectionRepartitioned
                .flatMapToPair(G32HM2_speedtest::countSingleWordsFromString)
                .groupBy(G32HM2_speedtest::assignRandomKey)
                .flatMapToPair(G32HM2_speedtest::aggregateWCperKeySubset)
                .reduceByKey(Long::sum);

        dWordCount2Pairs.cache().count();
        //waitHere();

        end = System.currentTimeMillis();
        speedTest.add(end-start);

        /*---------Word Count 2.2-------*/
        start = System.currentTimeMillis();

        JavaPairRDD<String, Long> dWordCount2Pairs2 =  collectionRepartitioned
                .mapPartitionsToPair(G32HM2_speedtest::wordCountInPartition2)
                .reduceByKey(Long::sum);

        dWordCount2Pairs2.cache().count();
        //waitHere();

        end = System.currentTimeMillis();
        speedTest.add(end-start);

        /*-------------------------------*/

        /*
        //We can also print the pairs with this - WATCH OUT: output are big files!
        printWordCount(dWordCount1Pairs,"wc1output.txt");
        printWordCount(dWordCount2Pairs,"wc2_1output.txt");
        printWordCount(dWordCount2Pairs2,"wc2_2output.txt");
        */

        /* Prints the average length of the distinct words appearing in the documents */
        long numberOfWoccurrences = dWordCount2Pairs2.count();
        long entireWordLength = dWordCount2Pairs2.map((x) -> Long.valueOf(x._1().length())).reduce(Long::sum);
        float averageLenghtOfDistW = (float) entireWordLength / numberOfWoccurrences;
        System.out.printf("Average length of the distinct words in the collection: %f characters\n", averageLenghtOfDistW);

        /* Print the time speed test */
        System.out.println("" +
                "\n------ Algorithm time measurement ------\n" +
                "Improved count 1: "+speedTest.get(0)+" ms\n" +
                "Improved count 2.1: "+speedTest.get(1)+" ms\n" +
                "Improved count 2.2: "+speedTest.get(2)+" ms\n" +
                "----------------------------------------");
    }

    /**
     * This function receives a subset and and count the words in that subset.
     *
     * @param subsetbykey a subset of keys
     * @return word count in the subset
     */
    private static Iterator<Tuple2<String,Long>> aggregateWCperKeySubset(Tuple2<Long, Iterable<Tuple2<String, Long>>> subsetbykey) {
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
    private static Long assignRandomKey(Tuple2<String,Long> s){
        return ThreadLocalRandom.current().nextLong(0, (int) Math.sqrt(k));
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
