import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Scanner;

public class G32HM1 {

    private static Double findMax(Double x, Double y) {
        if(x>=y){
            return x;
        }else{
            return y;
        }

    }

    public static class valueComparator implements Serializable, Comparator<Double> {

        public int compare(Double a, Double b){
            if(a < b) return -1;
            else if(a>b) return 1;
            else return 0;
        }

    }

    public static void main(String[] args) throws FileNotFoundException{

        if(args.length == 0){
            throw new IllegalArgumentException("Expecting the fie name on the command line");
        }

        ArrayList<Double> lNumbers = new ArrayList<>();

        //read a list of numbers from the file passed as argument
        Scanner s = new Scanner(new File(args[0]));
        while(s.hasNext()){
            lNumbers.add(Double.parseDouble(s.next()));
        }
        s.close();

        //setup spark local
        //setMaster("local") can be added if not set on vm options
        SparkConf configuration = new SparkConf(true).setAppName("G32HM1");

        JavaSparkContext sparkContext = new JavaSparkContext(configuration);

        //parallel collection
        JavaRDD<Double> dNumbers = sparkContext.parallelize(lNumbers);

        //finds and prints the maximum number using reduce method
        double maxNumberReduce = dNumbers.reduce(G32HM1::findMax);
        System.out.println("Reduce method: the max number is "+maxNumberReduce);

        //finds and prints the maximum number using max method
        double maxNumberMax = dNumbers.max(new valueComparator());
        System.out.println("Max method: the max number is "+maxNumberMax);

        //find the min value used for normalization
        double minNumberMin = dNumbers.min(new valueComparator());
        System.out.println("min: "+minNumberMin);

        //creates a RDD of normalized numbers
        JavaRDD<Double> dNormalized = dNumbers.map(x -> (x-minNumberMin)/(maxNumberMax-minNumberMin));

        /*
        The following code compute and print median value of the RDD dNormalized
        */

        //count how many elements contains
        long count = dNormalized.count();
        System.out.println("RDD contains "+count+" objects");

        //sum the value of all elements
        double sum = 0;
        List<Double> lNormalized = dNormalized.collect();
        for(Double listElement: lNormalized){
                    //System.out.println(listElement);
                    sum += listElement;
        }

        //compute the median value
        double media = sum/count;
        System.out.println("Il valore medio è: "+media);

    }

}
