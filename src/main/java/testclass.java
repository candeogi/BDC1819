import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class testclass {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf(true)
                .setAppName("testclass")
                .setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> collection = sc.textFile(args[0]);
        JavaRDD<String> repartition = collection.repartition(2);
        JavaRDD<List<String>> glom = repartition.glom();
        //List<List<String>> collect = glom.cache().collect();
        JavaRDD<String> objectJavaRDD = glom.mapPartitions(testclass::mapOperation);
        System.out.println("asd: "+objectJavaRDD.count());

    }

    private static Iterator<String> mapOperation(Iterator<List<String>> listIterator) {
        ArrayList<String> myarraylist = new ArrayList<>();
        for (Iterator<List<String>> it = listIterator; it.hasNext(); ) {
            for(String singleLineDoc : it.next()){
                myarraylist.add(singleLineDoc);
                System.out.println(singleLineDoc);
            }
        }
        return myarraylist.iterator();
    }
}