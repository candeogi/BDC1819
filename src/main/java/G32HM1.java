import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.execution.columnar.DOUBLE;
import org.codehaus.janino.Java;

import javax.print.DocFlavor;
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
        //setMaster("local") can be removed if set on run configuration
        SparkConf configuration = new SparkConf(true).setAppName("G32HM1").setMaster("local");

        JavaSparkContext sparkContext = new JavaSparkContext(configuration);

        //parallel collection
        JavaRDD<Double> dNumbers = sparkContext.parallelize(lNumbers);

        double maxNumberReduce = dNumbers.reduce(G32HM1::findMax);
        System.out.println("Reduce method: the max number is "+maxNumberReduce);


//        double maxNumberMax = dNumbers.max(new valueComparator());
//        System.out.println("Max method: the max number is "+maxNumberMax);

        //creates a RDD of normalized numbers
        JavaRDD<Double> dNumbersNormalized = dNumbers.map(x -> x/maxNumberReduce);

        //prints the content of the RDD - is this a statistic?
        List<Double> lNumbersNormalized = dNumbersNormalized.collect();
        lNumbersNormalized.forEach((temp)->{
            System.out.println(temp);
        });

    }
}
