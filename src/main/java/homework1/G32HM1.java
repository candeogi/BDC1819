package homework1;

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


/**
 * Group 32
 * File for the Homework n.1 of "Big Data Computing" Course"
 * The following file:
 * 1-reads an input file of non negative doubles into a RDD
 * 2-computes and prints the maximum value in 2 ways
 * 3-creates a RDD of normalized values
 * 4-computes and prints the mean value using count and collect methods
 *
 * @author Giovanni Candeo 1206150
 * @author Nicolo Levorato 1156744
 *
 */
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
        SparkConf configuration = new SparkConf(true).setAppName("homework1.G32HM1");

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

        //creates a RDD of normalized numbers
        JavaRDD<Double> dNormalized = dNumbers.map(x -> (x-minNumberMin)/(maxNumberMax-minNumberMin));

        /*
        The following code compute and print mean value of the RDD dNormalized
        */

        //count how many elements contains
        long count = dNormalized.count();

        // I could use this but instead lets use the collect method
        // double sum = dNormalized.reduce((x,y) -> x+y);

        //sum the value of all elements
        double sum = 0;
        List<Double> lNormalized = dNormalized.collect();
        for(Double listElement: lNormalized){
                    sum += listElement;
        }

        //compute the mean value
        double mean = sum/count;
        System.out.println("The mean value is: "+mean);

    }

}
