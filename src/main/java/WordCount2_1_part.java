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
import java.util.concurrent.ThreadLocalRandom;

/**
 * WordCount2
 * version 1
 * @author Giovanni Candeo
 */
public class WordCount2_1_part {

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
        collection.repartition(k);

        //lets start measuring time from here
        long start = System.currentTimeMillis();

        /* LONG VERSION WITH COLLECT METHODS FOR DEBUGGING */

        //collection already takes single Strings/Documents parallelized
        //List<String> coolCalmAndCollected= collection.collect();

        JavaPairRDD<String, Long> singleWordsRDD = collection.flatMapToPair(WordCount2_1_part::countSingleWordsFromDocString);
        //List<Tuple2<String, Long>> collectSingleWordsRDD = singleWordsRDD.collect();

        JavaPairRDD<String, Long> singleWordsRDDpart = collection
                .mapPartitionsToPair(WordCount2_1_part::countSingleWordsFromDocStringPart, true);

        //assign a random key to each document
        JavaPairRDD<Integer, Iterable<Tuple2<String, Long>>> subsetByKey = singleWordsRDDpart.groupBy(WordCount2_1_part::assignRandomKey);
        //List<Tuple2<Integer, Iterable<Tuple2<String, Long>>>> collectSubsetByKey = subsetByKey.collect();

        JavaPairRDD<String, Long> wordCountWordKey = subsetByKey.flatMapToPair(WordCount2_1_part::sumWOccurrencesOfKSubsets);
        //List<Tuple2<String, Long>> collectedWordCountWordKey = wordCountWordKey.collect();

        JavaPairRDD<String, Long> dWordCount2Pairs = wordCountWordKey.reduceByKey(Long::sum);
        //List<Tuple2<String, Long>> dWordCount2PairsCollected = dWordCount2Pairs.collect();


        /*
        JavaPairRDD<String, Long> dWordCount2Pairs = collection
                .flatMapToPair(WordCount2_1::countSingleWordsFromDocString)
                .groupBy(WordCount2_1::assignRandomKey)
                .flatMapToPair(WordCount2_1::sumWOccurrencesOfKSubsets)
                .reduceByKey(Long::sum);
        */

        dWordCount2Pairs.cache();
        System.out.println(dWordCount2Pairs.count());

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

