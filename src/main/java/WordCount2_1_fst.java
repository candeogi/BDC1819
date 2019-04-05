import org.apache.hadoop.util.hash.Hash;
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
 * WordCount2 - Version 2
 *
 * This variant that does not explicitly assign random keys but exploits the subdivision of docs into K parts in
 * combination with mapPartitionToPair to access each partition separately.
 * Again, K is the value given in input.
 * Note that if docs was initially partitioned into K parts, then, even after transformations that act on individual
 * elements,the resulting RDD stays partitioned into K parts and you may exploit this partition.
 * However, you can also invoke repartition(K) to reshuffle the RDD elements at random.
 *
 * The test-sample file contains documents composed by a single line of words.
 * #docs 10122
 * #words 3503570
 *
 * groupBy -> flatmaptopair -> reducebykey
 * @author Giovanni Candeo
 */
public class WordCount2_1_fst {

    private static long docWordOccurrences = 3503570;
    private static int k = 0;

    /**
     * Main method
     * @param args args[0] contains the name of the file txt we want to count
     *             args[1] contains the number of k partitions
     */
    public static void main(String[] args){

        if(args.length == 0){
            throw new IllegalArgumentException("expecting the file name");
        }

        SparkConf conf=new SparkConf(true)
                .setAppName("WordCount2")
                .setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        //RDD contains 10122 docs composed by a single line of numerous strings
        JavaRDD<String> collection = sc.textFile(args[0]).cache();
        System.out.println("Test count: "+collection.count());

        //number of partitions K, received as an input in the command line
        k = Integer.parseInt(args[1]);

        //-------------TIME MEASURE START ------------------
        long start = System.currentTimeMillis();

        JavaPairRDD<String, Long> dWordCount2partition =  collection
                .repartition(k)
                .mapPartitionsToPair(WordCount2_1_fst::wordCountInPartition)
                .reduceByKey(Long::sum);

        dWordCount2partition.cache();
        dWordCount2partition.count();
        //System.out.println(dWordCount2partition.count());

        //waitabit();

        //-------------TIME MEASURE END --------------------
        long end = System.currentTimeMillis();
        System.out.println("Elapsed time: " + (end - start) + " ms");

        //printWordCount(dWordCount2partition);

    }

    /**
     * this function operate on single partitions
     * @param documentsPartitioned single partition of documents.
     * @return word count occurrences in the partition
     **/
    private static Iterator<Tuple2<String,Long>> wordCountInPartition(Iterator<String> documentsPartitioned) {
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
     * Assigns a random key value at the input document
     *
     * @param s document
     * @return random key value between 0 and sqrt(k)
     */
    private static Long assignRandomKey2(String s) {
        return ThreadLocalRandom.current().nextLong(0, (int) Math.sqrt(k));
    }

    /**
     * This function is applied only to the single partition.
     * Counts and Sums the occurrences of words within a partition of documents.
     * Basically implements the first round of Word Count 2
     *
     * @param partition contains(x,[W1,W2,...,Wk])
     *                  where x is a random key
     *                  [W1,W2 .. ] is the partition of docs with assigned key
     * @return ritorna nuove coppie in cui la word è la chiave.
     */
    private static Iterator<Tuple2<String, Long>> wordCountInPartition(Tuple2<Long, Iterable<String>> partition) {
        ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
        HashMap<String, Long> counts = new HashMap<>();
        //per ogni documento conto le parole
        for(String document: partition._2()){
            String[] tokens = document.split(" ");
            for (String token : tokens) {
                //counts.put(token, 1L + counts.getOrDefault(token, 0L));
                counts.merge(token,1L,Long::sum);
            }
        }
        for (Map.Entry<String, Long> e : counts.entrySet()) {
            pairs.add(new Tuple2<>(e.getKey(), e.getValue()));
        }
        return pairs.iterator();
    }


    private static Iterator<Tuple2<String,Long>> countSingleWordsFromDocStringPart(Iterator<String> docPartIterator) {
        HashMap<String, Long> counts = new HashMap<>();
        ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
        while(docPartIterator.hasNext()){
            String documentsPartition = docPartIterator.next();
            String[] tokens = documentsPartition.split(" ");
            for (String token : tokens) {
                counts.put(token, 1L + counts.getOrDefault(token, 0L));
            }
            for (Map.Entry<String, Long> e : counts.entrySet()) {
                pairs.add(new Tuple2<>(e.getKey(), e.getValue()));
            }
        }
        return pairs.iterator();
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


    /**
     * Utility function
     * Prints the RDD to visualize the word count pairs
     * @param dWordCountPairsRDD
     */
    private static void printWordCount(JavaPairRDD<String,Long> dWordCountPairsRDD){
        Map<String, Long> lWordCountPairs = dWordCountPairsRDD.collectAsMap();
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
    }

    /**
     * Utility function
     * Stops the execution of the program to interact with the Spark webapp.
     */
    private static void waitabit(){
        System.out.println("press enter to finish the program");
        try{
            System.in.read();
        }catch (IOException e) {
            e.printStackTrace();
        }
    }
}

