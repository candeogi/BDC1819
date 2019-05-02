import org.apache.spark.mllib.linalg.BLAS;
import org.apache.spark.mllib.linalg.DenseVector;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;


import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.spark.mllib.linalg.Vectors.zeros;


/**
 * We will work with points in Euclidean space represented by vectors of reals.
 * In Spark, they can be represented as instances of the class org.apache.spark.mllib.linalg.Vector
 * and can be manipulated through static methods offered by the class org.apache.spark.mllib.linalg.Vectors.
 */
public class G32HM3 {
    public static void main(String[] args) {

        //reads input file "covtype10K.data"
        //covtype.data contains 10000 points in 55-dimensional euclidean space
        if (args.length == 0) {
            throw new IllegalArgumentException("expecting the file name");
        }
        ArrayList<Vector> P = new ArrayList<>();
        try {
            P = readVectorsSeq(args[0]);
        } catch (IOException e) {
            e.printStackTrace();
        }

        int k  = Integer.parseInt(args[1]);
        int iter = Integer.parseInt(args[2]);

        ArrayList<Double> myWeights = new ArrayList<>();

        /* Runs kmeansPP with weights equal to 1 */
        ArrayList<Vector> C = kmeansPP(P, myWeights, k, iter);

        double avgDistance = kmeansObj(P,C);
        /* compute the avg distance between points and centers */
        System.out.println("avg distance: "+avgDistance);

        /*
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

        double c = 2.0;

        //assigns y+c*x to y
        axpy(c,x,y);
        //assigns c*x to x
        scal(c,x);
        //assigns a copy of x to y
        copy(x,y);

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
     * @return C a set of centers
     */
    public static ArrayList<Vector> kmeansPP(ArrayList<Vector> P, ArrayList<Double> WP, int k, int iter){
        //set of centers
        ArrayList<Vector> C1 = new ArrayList<>();

        for(int i=0; i<P.size();i++){
            if(i == 2 ){
                WP.add(6.0);
            }else {
                WP.add(1.0);
            }
        }

        for(int i=0; i< WP.size() ;i++){
            for(int j=0; j<WP.get(i)-1;j++){
                Vector vector = zeros(P.get(i).size());
                BLAS.copy(P.get(i),vector);
                P.add(vector);
            }
        }

        //pick first center
        int randomNum = ThreadLocalRandom.current().nextInt(0, P.size());
        Vector myPoint = P.get(randomNum);

        System.out.println("POINT "+myPoint+" HAS BEEN CHOSEN (index = "+randomNum+")");
        C1.add(myPoint);

        P.remove(randomNum);

        //choose k-1 remaining centers with probability based on weight (and distance)
        for(int i = 2; i <=k; i++){
            System.out.println("-------start cycle ----------");

            double sum=0;
            //random number between 0 and 1
            double randomPivot = ThreadLocalRandom.current().nextDouble(0, 1);
            System.out.println("randomPivot: "+randomPivot );
            //for each point in "P-S" lets compute the range that will choose him over another
            for(int j = 0; j < P.size(); j++){
                sum =  sum + distance(P.get(j),C1);
            }

            double currentRange = 0;
            int chosenIndex = 0;
            boolean indexIsChosen = false;

            //choose the random point
            for(int j = 0; j < P.size(); j++){
                double probOfChoosingJ = (distance(P.get(j),C1) / sum);
                System.out.println("probOfChoosing "+P.get(j)+" is "+probOfChoosingJ);
                System.out.print("currentRange :"+currentRange);
                currentRange = currentRange + probOfChoosingJ;
                System.out.println(" - "+currentRange+" ");
                if((currentRange >= randomPivot)&&(!indexIsChosen)){
                    System.out.println("currentRange >= randomPivot");
                    chosenIndex = j;
                    indexIsChosen = true;
                }
            }
            System.out.println("currentrange should be 1 " +currentRange);

            myPoint = P.get(chosenIndex);
            System.out.println("POINT "+myPoint+" HAS BEEN CHOSEN (index = "+chosenIndex+")");

            C1.add(myPoint);
            P.remove(chosenIndex);

            System.out.println("-------end cycle ----------");
        }
        System.out.println("Set of centers:" +C1);
        //C1 now contains the centers

        //Partition(P,C1);
        //we want to apply iter iterations of Lloyds algorithm to get better centers
        System.out.println("-------Lloyds------");
        //We need to extract the clusters from the
        //The centroid of a cluster C is
        //(1/sum_{p in C} w(p)) * sum_{p in C} p*w(p)
        ArrayList<Vector> C = new ArrayList<>();
        for(Vector center : C1){
            C.add(center);
        }


        //only a number of iteration equal to "iter" parameter
        for(int j = 0; j < iter; j++){
            System.out.println("---- LLOYDS ITERATION N."+j+" ------");
            ArrayList<ArrayList<Vector>> partition = Partition(P, C);
            //compute the centroid for each partition
            for(int i = 0; i < partition.size(); i++){
                ArrayList<Vector> cluster = partition.get(i);
                Vector centroid = cluster.get(0);
                double[] centroidArray = centroid.toArray();
                //System.out.println("initial centroid value: "+centroid);
                for(k=1; k<cluster.size();k++){
                    Vector point = cluster.get(k);
                    double[] pointarray = point.toArray();
                    //System.out.println("point: "+point);
                    for(int o = 0; o < centroidArray.length; o++ ){
                        centroidArray[o] = centroidArray[o] + pointarray[o];
                    }
                    //System.out.println("centroid after axpy: "+centroid);
                }
                //assigns 1/sum_{p in C} * centroid to centroid
                //System.out.println("cluster size: "+cluster.size());
                for(int o = 0; o < centroidArray.length; o++ ){
                    centroidArray[o] = centroidArray[o]/cluster.size();
                }
                System.out.println("centroid of the "+i+" cluster is: "+centroid);
                Vector center = C.get(i);
                C.remove(0);
                P.add(center);
                C.add(centroid.copy());
            }
            System.out.println("C set of centers is: "+C);
        }

        /*
        //only a number of iteration equal to "iter" parameter
        for(int j = 0; j < iter; j++){
            System.out.println("---- LLOYDS ITERATION N."+j+" ------");
            ArrayList<ArrayList<Vector>> partition = Partition(P, C);
            //compute the centroid for each partition
            for(int i = 0; i < partition.size(); i++){
                ArrayList<Vector> cluster = partition.get(i);
                Vector centroid = cluster.get(0);
                System.out.println("initial centroid value: "+centroid);
                for(k=1; k<cluster.size();k++){
                    Vector point = cluster.get(k);
                    System.out.println("point: "+point);
                    BLAS.axpy(1.0,point,centroid);
                    System.out.println("centroid after axpy: "+centroid);
                }
                //assigns 1/sum_{p in C} * centroid to centroid
                System.out.println("cluster size: "+cluster.size());
                BLAS.scal(1/cluster.size(),centroid);
                System.out.println("centroid of the "+i+" cluster is: "+centroid);
                C.set(i,centroid.copy());
            }
        }

         */
        return C1;
    }

    /**
     * Partition primitive
     * @param P pointset
     * @param S set of k-selected centers
     * @return k-clustering of P
     */
    private static ArrayList<ArrayList<Vector>> Partition(ArrayList<Vector> P,ArrayList<Vector> S){
        ArrayList<ArrayList<Vector>> clusters = new ArrayList<>();
        //k-clustering
        int k = S.size();
        for(int i = 0; i < k; i++){
            clusters.add(new ArrayList<>());
            clusters.get(i).add(S.get(i));
        }
        for(Vector p : P){
            double minDistance = Double.MAX_VALUE;
            int l =0;
            //lets find at which centers p belongs
            for(int i = 0; i < k; i++){
                double distance = Math.sqrt(Vectors.sqdist(p, S.get(i)));
                if(distance < minDistance){
                    minDistance = distance;
                    l = i;
                }
            }
            clusters.get(l).add(p);
        }
        System.out.println("PARTITION: -- CLUSTERS PRINT TEST");
        for(ArrayList<Vector> cluster: clusters){
            System.out.println("cluster: "+cluster);
        }
        return clusters;
    }

    /**
     * Compute the distance between a point and a set of points.
     * @param vector point for which we want to calculate the distance from the set of centers
     * @param S set of centers
     * @return
     */
    private static double distance2(Vector vector, ArrayList<Vector> S) {
        int counter = 0;
        double sumDistance = 0;
        for(Vector center: S){
            sumDistance = sumDistance + Math.sqrt(Vectors.sqdist(vector,center));
            counter ++;
        }
        return sumDistance/counter;
    }

    private static double distance(Vector vector, ArrayList<Vector> S) {
        double minDistance = Double.MAX_VALUE;
        for(Vector center: S){
            double distance = Math.sqrt(Vectors.sqdist(vector,center));
            if(distance < minDistance){
                minDistance = distance;
            }
        }
        return minDistance;
    }



    /**
     * Receives in input a set of points P and a set of centers C,
     * and returns the average distance of a point of P from C
     * @param p
     * @param c
     */
    private static double kmeansObj(ArrayList<Vector> p, ArrayList<Vector> c) {
        double test = 1.0;
        /*
          code
        */
        return test;
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
