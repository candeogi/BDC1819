import org.apache.spark.mllib.linalg.BLAS;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import static org.apache.spark.mllib.linalg.BLAS.*;

/**
 * We will work with points in Euclidean space represented by vectors of reals.
 * In Spark, they can be represented as instances of the class org.apache.spark.mllib.linalg.Vector
 * and can be manipulated through static methods offered by the class org.apache.spark.mllib.linalg.Vectors.
 */
public class HW3TEST {
    public static void main(String[] args) {

        double[] array1 = {1.0,1.0,1.0,1.0};
        double[] array2 = {2.0,2.0,2.0,2.0};

        //transforms an array x of double into an instance of class Vector
        Vector x = Vectors.dense(array1);
        Vector y = Vectors.dense(array2);

        //computes the (d(x,y))^2 between two Vector x and y,
        //where "d(.,.)" is the standard Euclidean L2-distance.
        Vectors.sqdist(x,y);

        double c = 2.0;

        //assigns y+c*x to y
        axpy(c,x,y);
        //assigns c*x to x
        scal(c,x);
        //assigns a copy of x to y
        copy(x,y);

        /*
        Be careful that given two Vector variables x and y,
        if you write y=x both variables will point to the same object,
        while if you write BLAS.copy(x,y) variable y points to a copy of x,
        hence x and y point to distinct objects.
         */
    }
}
