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
 * WordCount2
 * version 1
 * groupBy -> flatmaptopair -> reducebykey
 * @author Giovanni Candeo
 */
public class WordCount2_1_fst {

    private static long docWordOccurrences = 3503570;
    private static int k = 0;

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

        JavaPairRDD<String, Long> dWordCount2partition = collection
                .groupBy(WordCount2_1_fst::assignRandomKey2)
                .flatMapToPair(WordCount2_1_fst::wordCountInPartition)
                .reduceByKey(Long::sum);

        dWordCount2partition.cache();
        System.out.println(dWordCount2partition.count());

        //waitabit();

        //end of time measuring
        long end = System.currentTimeMillis();
        System.out.println("Elapsed time: " + (end - start) + " ms");
        printWordCount(dWordCount2partition);

    }

    /**
     * questo metodo lavora direttamente sulla singole partitions in parallelo
     * @param partition contiene (x,[W1,W2,...,Wk])
     *                           dove la x  è random key
     *                           [W1,W2 .. ] è la partizione di doc a cui è stata assegnata la key
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

    private static Long assignRandomKey2(String s) {
        return ThreadLocalRandom.current().nextLong(0, (int) Math.sqrt(k));
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

