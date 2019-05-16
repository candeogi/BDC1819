package templates_and_tests;

import org.apache.spark.mllib.linalg.BLAS;
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
public class G32HM3arrayList {
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

        ArrayList<Long> myWeights = new ArrayList<>();
        for(int i=0; i<P.size();i++){
            if(i==P.size()-1){
                //for test purposes
                myWeights.add(1L);
            }else{
                myWeights.add(1L);
            }
        }

        /* Runs kmeansPP with weights equal to 1 */
        ArrayList<Vector> C = kmeansPP(P, myWeights, k, iter);

        double avgDistance = kmeansObj(P,C);
        /* compute the avg distance between points and centers */
        //System.out.println("avg distance: "+avgDistance);

        /*
        //assigns y+c*x to y
        BLAS.axpy(c,x,y);
        //assigns c*x to x
        BLAS.scal(c,x);
        //assigns a copy of x to y
        BLAS.copy(x,y);
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
    public static ArrayList<Vector> kmeansPP(ArrayList<Vector> P, ArrayList<Long> WP, int k, int iter){
        //set of centers
        ArrayList<Vector> C1 = new ArrayList<>();

        //weights
        for(int i=0; i< WP.size() ;i++){
            for(int j=0; j<WP.get(i)-1;j++){
                Vector vector = zeros(P.get(i).size());
                BLAS.copy(P.get(i),vector);
                P.add(vector);
            }
        }

        //pick first center
        int randomNum = ThreadLocalRandom.current().nextInt(0, P.size());
        Vector randomPoint = P.get(randomNum);
        Vector randomCenter = zeros(randomPoint.size());
        BLAS.copy(randomPoint,randomCenter);
        C1.add(randomCenter);

        System.out.println("POINT "+randomPoint+" HAS BEEN CHOSEN (index = "+randomNum+")");

        //P.remove(randomNum);

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

            Vector probFarthestPoint = P.get(chosenIndex);
            Vector probFarthestNewCenter = zeros(probFarthestPoint.size());
            BLAS.copy(probFarthestPoint,probFarthestNewCenter);
            C1.add(probFarthestNewCenter);

            //P.remove(chosenIndex);

            System.out.println("NEW CENTER "+probFarthestNewCenter+" HAS BEEN CHOSEN");
            System.out.println("-------end cycle ----------");
        }
        System.out.println("Set of centers:" +C1);
        System.out.println("P is: "+P );
        //C1 now contains the centers

        //Partition(P,C1);
        //we want to apply iter iterations of Lloyds algorithm to get better centers
        System.out.println("-------Lloyds------");
        //We need to extract the clusters from the
        //The centroid of a cluster C is
        //(1/sum_{p in C} w(p)) * sum_{p in C} p*w(p)

        //ugly TODO change maybe
        ArrayList<ArrayList<Vector>> Centers = new ArrayList<>();
        Centers.add(C1);

        //only a number of iteration equal to "iter" parameter
        for(int j = 0; j < iter; j++){
            System.out.println("---- LLOYDS ITERATION N."+j+" ------");
            System.out.println("LLOYDS working on...");
            System.out.println("P: "+P);
            System.out.println("C("+j+") is: "+Centers.get(j));

            ArrayList<ArrayList<Vector>> partition = Partition(P, Centers.get(j));
            ArrayList<Vector> newCenters = new ArrayList<>();
            //compute the centroid for each partition
            for(int i = 0; i < partition.size(); i++){
                ArrayList<Vector> cluster = partition.get(i);
                Vector initPoint = cluster.get(0);
                Vector centroid = zeros(initPoint.size());
                BLAS.copy(initPoint,centroid);

                for(k=1; k<cluster.size();k++){
                    Vector point = cluster.get(k);
                    BLAS.axpy(1.0,point,centroid);
                }
                //assigns 1/sum_{p in C} * centroid to centroid
                double c = (double) 1/cluster.size();
                BLAS.scal(c,centroid);
                System.out.println("centroid of the "+i+" cluster is: "+centroid);
                Vector newCenter = zeros(centroid.size());
                BLAS.copy(centroid,newCenter);
                //create a new set of centers C(j) - up to C(iter)
                newCenters.add(newCenter);
            }
            Centers.add(newCenters);
        }

        System.out.println("@@@@@@ centers computed @@@@@@@");
        for(int i = 0; i<Centers.size();i++){
            System.out.println("C("+i+") is: "+Centers.get(i));
        }

        /*Test obj function*/
        for(ArrayList<Vector> setOfC: Centers){
            /* compute the avg distance between points and centers */
            System.out.println("avg distance: "+kmeansObj(P,setOfC));
        }

        return Centers.get(Centers.size()-1);
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
            //create a cluster for each center in S
            clusters.add(new ArrayList<>());
            //clusters.get(i).add(S.get(i));
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
            //the point P belongs to the cluster l
            clusters.get(l).add(p);
        }
        System.out.println("-------start print PARTITION-------");
        for(ArrayList<Vector> cluster: clusters){
            System.out.println("cluster: "+cluster);
        }
        System.out.println("----end print partition clusters---");
        return clusters;
    }


    /**
     * Compute the distance between a point and the closest center.
     * @param vector point for which we want to calculate the distance
     * @param S set of centers
     * @return minDistance the distance to the cloesest center
     */
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
        double sumDistance = 0;
        for(int i=0;i<p.size();i++){
            sumDistance = sumDistance + distance(p.get(i),c);
        }

        return sumDistance/p.size();
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
