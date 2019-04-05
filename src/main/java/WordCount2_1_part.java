import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.execution.columnar.LONG;
import org.codehaus.janino.Java;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;
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

        /*
        //-----------Long Version for debug
        JavaPairRDD<Long, Iterable<String>> docWassignedKey = collection.repartition(k).groupBy(WordCount2_1_part::assignRandomKey2);
        //List<Tuple2<Long, Iterable<String>>> collectedDocWassignedKey = docWassignedKey.collect();
        docWassignedKey.count();

        JavaPairRDD<String,Long> wordCountWordKeyPart =  docWassignedKey.flatMapToPair(WordCount2_1_part::wordCountInPartition);
        //List<Tuple2<String, Long>> collectcountwordkeypart = wordCountWordKeyPart.collect();
        wordCountWordKeyPart.count();

        JavaPairRDD<String, Long> dWordCount2partition = wordCountWordKeyPart.reduceByKey(Long::sum);
        //List<Tuple2<String, Long>> dWordCount2partitioncollected = dWordCount2partition.collect();
        //------------FINISH NEW STUFF
        */

        JavaPairRDD<String, Long> dWordCount2partition = collection
                .groupBy(WordCount2_1_part::assignRandomKey2)
                .flatMapToPair(WordCount2_1_part::wordCountInPartition)
                .reduceByKey(Long::sum);


        dWordCount2partition.cache();
        System.out.println(dWordCount2partition.count());

        //waitabit();

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
                counts.put(token, 1L + counts.getOrDefault(token, 0L));
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

    private static int assignRandomKey(Tuple2<String, Long> stringLongTuple2) {
        return ThreadLocalRandom.current().nextInt(0, (int) Math.sqrt(k));
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

}

