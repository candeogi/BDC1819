import org.apache.spark.mllib.linalg.BLAS;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.spark.mllib.linalg.Vectors.zeros;


/**
 * Group 32
 * File for the Homework n.3 of "Big Data Computing" Course"
 *
 * @author Giovanni Candeo 1206150
 * @author Nicolo Levorato 1156744
 *
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

        ArrayList<Long> myWeights = new ArrayList<>();
        //for this homework, weights are set to 1
        for(int i=0; i<P.size();i++){
            myWeights.add(1L);
        }

        System.out.println("File: "+args[0]);
        System.out.println("Centers: "+k);
        System.out.println("Iterations: "+iter);

        long start = System.currentTimeMillis();

        //runs kmeansPP with weights equal to 1
        ArrayList<Vector> C = kmeansPP(P, myWeights, k, iter);

        //compute the average minDistance between points and centers
        double avgDistance = kmeansObj(P,C);
        long end = System.currentTimeMillis();

        System.out.println("average minDistance = "+avgDistance);
        System.out.println("total time = "+(end-start));
    }



    /**
     * This method computes a set C of k centers computed as follows:
     *
     * Compute a first set C' of centers using the weighted variant of the kmeans++
     * In each iteration the probability for a non-center point p of being chosen as next center is:
     * w_p*(d_p)/(sum_{q non center} w_q*(d_q))
     * where d_p is the minDistance of p from the closest among the already selected centers and w_p is the weight of p.
     *
     * Then it applies the Lloyds' algorithm for up to an "iter" number of iterations or until it reaches a minimum
     * value of the objective function
     * The best set of centers are then returned.
     *
     * @param P set of points
     * @param WP weights WP of P
     * @param k number of centers
     * @param iter number of iterations of Lloyd's algorithm
     * @return C a set of centers
     */
    private static ArrayList<Vector> kmeansPP(ArrayList<Vector> P, ArrayList<Long> WP, int k, int iter){
        //set of centers
        ArrayList<Vector> C1 = new ArrayList<>();

        //an Hashmap is used to store and access the weights related to distinct vectors
        HashMap<Vector,Long> weightsOfP = new HashMap<>();

        //initialize the hash map containing P and its weights
        for(int i = 0; i<P.size(); i++){
            weightsOfP.put(P.get(i),WP.get(i));
        }

        //pick first center
        int randomNum = ThreadLocalRandom.current().nextInt(0, P.size());
        Vector randomPoint = P.get(randomNum);
        C1.add(randomPoint);

        //an Hashmap is used to store the minDistance of a point related to its closest center
        HashMap<Vector,Double> distancesOfP = new HashMap<>();

        //initialize the distances with the first center picked randomly
        for(Vector currentVector: P){
            distancesOfP.put(currentVector, euclidean(currentVector,randomPoint));
        }

        //choose k-1 remaining centers with probability based on its weight and minDistance
        for(int i = 2; i <=k; i++){

            //random number between 0 and 1
            double randomPivot = ThreadLocalRandom.current().nextDouble(0, 1);

            //compute the sum of distances
            double sumOfDistances=0;
            for(Vector currentVector : P){
                sumOfDistances =  sumOfDistances + distancesOfP.get(currentVector)*weightsOfP.get(currentVector);
            }

            //pick the new next center with probability based on its weight and minDistance
            double currentRange = 0;
            Vector probFarthestPoint = P.get(0);
            for(Vector currentVector : P){
                double probOfChoosing = (distancesOfP.get(currentVector)*weightsOfP.get(currentVector) / sumOfDistances);
                currentRange = currentRange + probOfChoosing;
                if(currentRange >= randomPivot){
                    probFarthestPoint = currentVector;
                    break;
                }
            }
            C1.add(probFarthestPoint);

            //update the min distances between points and new center
            for(Vector currentVector : P){
                double newDistance  = euclidean(currentVector,probFarthestPoint);
                if(newDistance<distancesOfP.get(currentVector)){
                    distancesOfP.put(currentVector,newDistance);
                }
            }
        }

        //C1 now contains the centers
        //we want to apply "iter" iterations of Lloyds' algorithm to get better centers

        //C is an "arraylist" that stores the list of centers computed in every iteration of Lloyds' alg.
        ArrayList<ArrayList<Vector>> C = new ArrayList<>();
        C.add(C1);

        /* Lloyds' algorithm */

        double minObjFuncValue = Double.MAX_VALUE;
        for(int j = 0; j < iter; j++){
            ArrayList<ArrayList<Vector>> partition = Partition(P, C.get(j));
            ArrayList<Vector> newCenters = new ArrayList<>();

            //compute the centroid for each partition
            for(int i = 0; i < partition.size(); i++){
                ArrayList<Vector> cluster = partition.get(i);

                //initialize the centroid
                Vector initPoint = cluster.get(0);
                Vector centroid = zeros(initPoint.size());
                BLAS.copy(initPoint,centroid);
                Long sumOfWeights = weightsOfP.get(initPoint);

                //update the centroid value for each point of the cluster
                for(k=1; k<cluster.size();k++){
                    Vector currentVector = cluster.get(k);
                    Long currentWeight = weightsOfP.get(currentVector);

                    //sum of weighted points
                    BLAS.axpy(currentWeight,currentVector,centroid);
                    sumOfWeights = sumOfWeights + currentWeight;
                }
                //assigns 1/sum_{p in C} * centroid to centroid
                double c = (double) 1/sumOfWeights;
                BLAS.scal(c,centroid);
                Vector newCenter = zeros(centroid.size());
                BLAS.copy(centroid,newCenter);

                //update the set of centers
                newCenters.add(newCenter);
            }

            //check if the objective function is decreasing
            double newObjFuncValue = kmeansObj(P, newCenters);

            //continues the lloyds' iteration only if its decreasing
            if(newObjFuncValue<minObjFuncValue){
                minObjFuncValue = newObjFuncValue;
                C.add(newCenters);
            }
            else{
                //the obj function is not decreasing, exit the lloyds iterations.
                System.out.println("Lloyd's ended earlier ---> Optimal obj function found in iteration n."+ j);
                break;
            }
        }

        /*
        for(int i = 0; i<C.size();i++){
            System.out.println("C("+i+") is: "+C.get(i));
            System.out.println("avg minDistance: "+kmeansObj(P,C.get(i)));
        }*/

        //return the last optimal set of centers
        return C.get(C.size()-1);
    }

    /**
     * The name of the function is kmeans as requested on the homework assignment but in fact should be kmedian.
     *
     * Receives in input a set of points P and a set of centers C,
     * and returns the average minDistance of a point of P from C
     *
     * @param p
     * @param c
     * @return average minDistance
     */
    private static double kmeansObj(ArrayList<Vector> p, ArrayList<Vector> c) {
        double sumDistance = 0;
        for(int i=0;i<p.size();i++){
            sumDistance = sumDistance + minDistance(p.get(i),c);
        }
        return sumDistance/p.size();
    }

    /**
     * Partition primitive
     *
     * A cluster is represented by an arraylist of points.
     * All clusters are stored into another arraylist.
     * The arraylist containing all clusters are returned.
     *
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
        return clusters;
    }

    /**
     * Euclidean minDistance between two vectors
     *
     * @param a vector
     * @param b vector
     * @return euclidean minDistance between two vectors
     */
    private static double euclidean(Vector a, Vector b) {
        return Math.sqrt(Vectors.sqdist(a, b));
    }

    /**
     * Compute the minDistance between a point and the closest center.
     *
     * @param vector point for which we want to calculate the minDistance
     * @param S set of centers
     * @return minDistance the minDistance to the closest center
     */
    private static double minDistance(Vector vector, ArrayList<Vector> S) {
        double minDistance = Double.MAX_VALUE;
        for(Vector center: S){
            double distance = euclidean(vector,center);
            if(distance < minDistance){
                minDistance = distance;
            }
        }
        return minDistance;
    }

    /**
     * String to Vector
     *
     * @param str
     * @return Vector representing the point
     */
    private static Vector strToVector(String str) {
        String[] tokens = str.split(" ");
        double[] data = new double[tokens.length];
        for (int i=0; i<tokens.length; i++) {
            data[i] = Double.parseDouble(tokens[i]);
        }
        return Vectors.dense(data);
    }

    /**
     * Converts a file representing a dataset of points into an arraylist of vectors
     *
     * @param filename input file
     * @return ArrayList of vectors representing the dataset
     * @throws IOException
     */
    private static ArrayList<Vector> readVectorsSeq(String filename) throws IOException {
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
