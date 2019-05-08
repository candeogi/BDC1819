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
        //for this hw, weights are set to 1
        for(int i=0; i<P.size();i++){
            myWeights.add(1L);
        }

        /* Runs kmeansPP with weights equal to 1 */
        ArrayList<Vector> C = kmeansPP(P, myWeights, k, iter);

        /* compute the avg distance between points and centers */
        double avgDistance = kmeansObj(P,C);
        System.out.println("avg distance: "+avgDistance);
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
    private static ArrayList<Vector> kmeansPP(ArrayList<Vector> P, ArrayList<Long> WP, int k, int iter){
        //set of centers
        ArrayList<Vector> C1 = new ArrayList<>();

        HashMap<Vector,Long> weightsOfP = new HashMap<>();
        HashMap<Vector,Double> distancesOfP = new HashMap<>();

        //initialize the hash map containing P and its weights
        for(int i = 0; i<P.size(); i++){
            weightsOfP.put(P.get(i),WP.get(i));
        }

        //pick first center
        int randomNum = ThreadLocalRandom.current().nextInt(0, P.size());
        Vector randomPoint = P.get(randomNum);
        C1.add(randomPoint);

        //choose k-1 remaining centers with probability based on its weight and distance
        for(int i = 2; i <=k; i++){

            //random number between 0 and 1
            double randomPivot = ThreadLocalRandom.current().nextDouble(0, 1);

            //compute the distances of the points from the centers
            double sumOfDistances=0;
            for(Vector currentVector : P){
                distancesOfP.put(currentVector, distance(currentVector,C1));
                sumOfDistances =  sumOfDistances + distancesOfP.get(currentVector)*weightsOfP.get(currentVector);
            }

            //pick the new next center with probability based on its weight and distance
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
        }
        //C1 now contains the centers
        //we want to apply "iter" iterations of Lloyds' algorithm to get better centers

        //C is an "arraylist" that stores the list of centers computed in every iteration of Lloyds' alg.
        //C1 = C(0) and so on ...
        ArrayList<ArrayList<Vector>> C = new ArrayList<>();
        C.add(C1);

        //only a number of iteration equal to "iter" parameter
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

            //check if the objective function gets better
            double newObjFuncValue = kmeansObj(P, newCenters);

            //continues the lloyds' iteration only if gets better
            if(newObjFuncValue<minObjFuncValue){
                minObjFuncValue = newObjFuncValue;
                C.add(newCenters);
            }
            else{
                //the obj function doesnt get better, exit the lloyds iterations.
                break;
            }
        }

        /*
        for(int i = 0; i<C.size();i++){
            System.out.println("C("+i+") is: "+C.get(i));
            System.out.println("avg distance: "+kmeansObj(P,C.get(i)));
        }*/

        //return the last optimal set of centers
        return C.get(C.size()-1);
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
     * @return average distance
     */
    private static double kmeansObj(ArrayList<Vector> p, ArrayList<Vector> c) {
        double sumDistance = 0;
        for(int i=0;i<p.size();i++){
            sumDistance = sumDistance + distance(p.get(i),c);
        }
        return sumDistance/p.size();
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
