import org.apache.spark.mllib.linalg.BLAS;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;

import static org.apache.spark.mllib.linalg.BLAS.*;

/**
 * We will work with points in Euclidean space represented by vectors of reals.
 * In Spark, they can be represented as instances of the class org.apache.spark.mllib.linalg.Vector
 * and can be manipulated through static methods offered by the class org.apache.spark.mllib.linalg.Vectors.
 */
public class HW3TEST {
    public static void main(String[] args) {

        if (args.length == 0) {
            throw new IllegalArgumentException("expecting the file name");
        }
        ArrayList<Vector> myVectors = new ArrayList<>();
        try {
            myVectors = readVectorsSeq(args[0]);
        } catch (IOException e) {
            e.printStackTrace();
        }

        int k  = Integer.parseInt(args[1]);
        int iter = Integer.parseInt(args[2]);


        double[] array1 = {0.0,0.0};
        double[] array2 = {1.0,1.0};

        //transforms an array x of double into an instance of class Vector
        Vector x = Vectors.dense(array1);
        Vector y = Vectors.dense(array2);

        //computes the (d(x,y))^2 between two Vector x and y,
        //where "d(.,.)" is the standard Euclidean L2-distance.
        double sqdist = Vectors.sqdist(x, y);
        double dist = Math.sqrt(sqdist);
        System.out.println(dist);

        /*
        double c = 2.0;

        //assigns y+c*x to y
        axpy(c,x,y);
        //assigns c*x to x
        scal(c,x);
        //assigns a copy of x to y
        copy(x,y);
        */

        /*
        Be careful that given two Vector variables x and y,
        if you write y=x both variables will point to the same object,
        while if you write BLAS.copy(x,y) variable y points to a copy of x,
        hence x and y point to distinct objects.
         */

    }

    /**
     * This method computes a set C of k centers computed as follows:
     * compute a first set C' of centers using the weighted variant of the kmeans++
     * In each iteration the probability for a non-center point p of being chosen as next center is:
     * w_p*(d_p)/(sum_{q non center} w_q*(d_q))
     * where d_p is the distance of p from the closest among the already selected centers and w_p is the weight of p.
     *
     *
     * @param P set of points
     * @param WP weights WP of P
     * @param k number of centers
     * @param iter number of iterations of Lloyd's algorithm
     * @return
     */
    public void kmeansPP(ArrayList<Vector> P, double[] WP, int k, int iter){
        int size = P.size();
        int randomPoint= (int) Math.random()*size;
        ArrayList<Vector> C = new ArrayList<>();

        //random first center
        C.add(P.get(randomPoint));
        P.remove(randomPoint);

        for(int i = 2; i < k ; i++){

        }

    }

    public static Vector strToVector(String str) {
        String[] tokens = str.split(" ");
        double[] data = new double[tokens.length];
        for (int i=0; i<tokens.length; i++) {
            data[i] = Double.parseDouble(tokens[i]);
        }
        return Vectors.dense(data);
    }

    public static ArrayList<Vector> readVectorsSeq(String filename) throws IOException {
        if (Files.isDirectory(Paths.get(filename))) {
            throw new IllegalArgumentException("readVectorsSeq is meant to read a single file.");
        }
        ArrayList<Vector> result = new ArrayList<>();
        Files.lines(Paths.get(filename))
                .map(str -> strToVector(str))
                .forEach(e -> result.add(e));
        return result;
    }

}
