package templates_and_tests;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Scanner;

public class FirstTemplate {

    public static double squareIt(double number){
        return number * number;
    }

    private static double sumIt(Double x, Double y) {
        return x+y;
    }

    public static void main(String[] args) throws FileNotFoundException {

        if(args.length == 0){
            throw new IllegalArgumentException("Expecting the file name on the command line");
        }

        // Read a list of numbers from the program options
        ArrayList<Double> lNumbers = new ArrayList<>();
        Scanner s =  new Scanner(new File(args[0]));

        while (s.hasNext()){
            lNumbers.add(Double.parseDouble(s.next()));
        }
        s.close();

        // Setup Spark
        SparkConf conf = new SparkConf(true).setAppName("Preliminaries").setMaster("local");
        //.setMaster("local")

        JavaSparkContext sc = new JavaSparkContext(conf);

        // Create a parallel collection
        JavaRDD<Double> dNumbers = sc.parallelize(lNumbers);


        double sumOfSquares = dNumbers
                                .map(FirstTemplate::squareIt)
                                .reduce(FirstTemplate::sumIt);

        System.out.println("The sum of squares is " + sumOfSquares);

    }

}
