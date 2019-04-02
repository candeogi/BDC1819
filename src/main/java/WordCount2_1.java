import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * WordCount2
 * version 1
 * @author Giovanni Candeo
 */
public class WordCount2_1 {

    private static long docWordOccurrences = 3503570;

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
        int k = Integer.parseInt(args[1]);

        //lets start measuring time from here
        long start = System.currentTimeMillis();

        JavaPairRDD<String,Long> dWordCountPairs =collection
                .repartition(k)
                .glom()
                .mapPartitions(WordCount2_1::test);
        /*
        JavaPairRDD<Long,Iterable<List<String>>> dWordCountPairs = collection
                .repartition(k)
                .glom()
                .groupBy(WordCount2_1::assignRandomKey);
        */
                /*.reduceByKey((iterableOfString)->{
                    //iterableOfString contains all documents
                })
                .flatMapToPair(WordCount2_1::countSingleWords)
                .reduceByKey(Long::sum);*/

        //i need this for computing the actual RDD transformation
        dWordCountPairs.cache();
        System.out.println("ASD TEST COUNT:" +dWordCountPairs.count());

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

    private static Iterator<String> test(List<String> stringIterator) {
        for(String wtfisthis : stringIterator){

        }
    }

    private static Long assignRandomKey(List<String> strings) {
        return ThreadLocalRandom.current().nextLong(0,  docWordOccurrences);
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

