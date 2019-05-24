import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.BLAS;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.execution.columnar.DOUBLE;
import scala.Array;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.spark.mllib.linalg.Vectors.parse;
import static org.apache.spark.mllib.linalg.Vectors.zeros;

public class G32HM4
{
    public static void main(String[] args) throws Exception
    {

        //------- PARSING CMD LINE ------------
        // Parameters are:
        // <path to file>, k, L and iter

        if (args.length != 4) {
            System.err.println("USAGE: <filepath> k L iter");
            System.exit(1);
        }
        String inputPath = args[0];
        int k=0, L=0, iter=0;
        try
        {
            k = Integer.parseInt(args[1]);
            L = Integer.parseInt(args[2]);
            iter = Integer.parseInt(args[3]);
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
        if(k<=2 && L<=1 && iter <= 0)
        {
            System.err.println("Something wrong here...!");
            System.exit(1);
        }
        //------------------------------------
        final int k_fin = k;

        //------- DISABLE LOG MESSAGES
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        //------- SETTING THE SPARK CONTEXT      
        SparkConf conf = new SparkConf(true).setAppName("kmedian new approach");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //------- PARSING INPUT FILE ------------
        JavaRDD<Vector> pointset = sc.textFile(args[0], L)
                .map(x-> strToVector(x))
                .repartition(L)
                .cache();
        long N = pointset.count();
        System.out.println("");
        System.out.println("Number of points is : " + N);
        System.out.println("Number of clusters is : " + k);
        System.out.println("Number of parts is : " + L);
        System.out.println("Number of iterations is : " + iter);

        //------- SOLVING THE PROBLEM ------------
        double obj = MR_kmedian(pointset, k, L, iter);
        System.out.println("Objective function is : <" + obj + ">");
    }

    public static Double MR_kmedian(JavaRDD<Vector> pointset, int k, int L, int iter)
    {
        // INSTRUCTIONS TO TAKE AND PRINT TIMES OF ROUNDS 1, 2 and 3
        ArrayList<Long> speedTest = new ArrayList<>();
        long start;
        long end;

        pointset.cache().count();
        //------------- ROUND 1 ---------------------------
        start = System.currentTimeMillis();

        JavaRDD<Tuple2<Vector,Long>> coreset = pointset.mapPartitions(x ->
        {
            ArrayList<Vector> points = new ArrayList<>();
            ArrayList<Long> weights = new ArrayList<>();
            while (x.hasNext())
            {
                points.add(x.next());
                weights.add(1L);
            }
            ArrayList<Vector> centers = kmeansPP(points, weights, k, iter);
            ArrayList<Long> weight_centers = compute_weights(points, centers);
            ArrayList<Tuple2<Vector,Long>> c_w = new ArrayList<>();
            for(int i =0; i < centers.size(); ++i)
            {
                Tuple2<Vector, Long> entry = new Tuple2<>(centers.get(i), weight_centers.get(i));
                c_w.add(i,entry);
            }
            return c_w.iterator();
        });

        coreset.cache().count();
        end = System.currentTimeMillis();
        speedTest.add(end-start);

        //------------- ROUND 2 ---------------------------

        start = System.currentTimeMillis();

        ArrayList<Tuple2<Vector, Long>> elems = new ArrayList<>(k*L);
        elems.addAll(coreset.collect());
        ArrayList<Vector> coresetPoints = new ArrayList<>();
        ArrayList<Long> weights = new ArrayList<>();
        for(int i =0; i< elems.size(); ++i)
        {
            coresetPoints.add(i, elems.get(i)._1);
            weights.add(i, elems.get(i)._2);
        }

        ArrayList<Vector> centers = kmeansPP(coresetPoints, weights, k, iter);

        end = System.currentTimeMillis();
        speedTest.add(end-start);
        //------------- ROUND 3: COMPUTE OBJ FUNCTION --------------------
        start = System.currentTimeMillis();

        double objFuncValue = kmeansObj(pointset, centers);

        end = System.currentTimeMillis();
        speedTest.add(end-start);

        long totalTime = speedTest.get(0) + speedTest.get(1) + speedTest.get(2);
        /* Print the time speed test */
        System.out.println("" +
                "\n------ Round time measurement ------\n" +
                "Round 1: "+speedTest.get(0)+" ms\n" +
                "Round 2: "+speedTest.get(1)+" ms\n" +
                "Round 3: "+speedTest.get(2)+" ms\n" +
                "Total: "+totalTime+" ms\n" +
                "----------------------------------------\n");

        return objFuncValue;
    }



    public static ArrayList<Long> compute_weights(ArrayList<Vector> points, ArrayList<Vector> centers)
    {
        Long weights[] = new Long[centers.size()];
        Arrays.fill(weights, 0L);
        for(int i =0; i < points.size(); ++i)
        {
            double tmp = euclidean(points.get(i), centers.get(0));
            int mycenter = 0;
            for(int j = 1; j < centers.size(); ++j)
            {
                if(euclidean(points.get(i),centers.get(j)) < tmp)
                {
                    mycenter = j;
                    tmp = euclidean(points.get(i), centers.get(j));
                }
            }
            weights[mycenter] += 1L;
        }
        ArrayList<Long> fin_weights = new ArrayList<>(Arrays.asList(weights));
        return fin_weights;
    }

    public static Vector strToVector(String str) {
        String[] tokens = str.split(" ");
        double[] data = new double[tokens.length];
        for (int i = 0; i < tokens.length; i++) {
            data[i] = Double.parseDouble(tokens[i]);
        }
        return Vectors.dense(data);
    }

    // Euclidean distance
    public static double euclidean(Vector a, Vector b) {
        return Math.sqrt(Vectors.sqdist(a, b));
    }

    /**
     * This method computes a set C of k centers computed as follows:
     *
     * Compute a first set C' of centers using the weighted variant of the kmeans++
     * In each iteration the probability for a non-center point p of being chosen as next center is:
     * w_p*(d_p)/(sum_{q non center} w_q*(d_q))
     * where d_p is the distance of p from the closest among the already selected centers and w_p is the weight of p.
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

        //choose k-1 remaining centers with probability based on its weight and distance
        for(int i = 2; i <=k; i++){

            //an Hashmap is used to store the distance of a point related to its closest center
            //this Hashmap is updated every iteration
            HashMap<Vector,Double> distancesOfP = new HashMap<>();

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
        ArrayList<ArrayList<Vector>> C = new ArrayList<>();
        C.add(C1);

        /* Lloyds' algorithm */

        for(int j = 0; j < iter; j++){
            System.out.println("Lloyd iteration= "+j);
            ArrayList<ArrayList<Vector>> partition = Partition(P, C.get(j));
            ArrayList<Vector> newCenters = new ArrayList<>();
            System.out.println("---------------------");
            System.out.println("centers = "+C.get(j));
            for(Vector center : C.get(j)){
                System.out.println(center);
            }
            System.out.println("---------------------");
            System.out.println("partition = "+partition);
            for(ArrayList<Vector> oneCluster : partition){
                System.out.println(oneCluster);
            }
            System.out.println("---------------------");
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

            C.add(newCenters);
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
     *
     * Receives in input a set of points P and a set of centers C,
     * and returns the average distance of a point of P from C
     *
     * @param pointset RDD of vectors
     * @param centers arraylist of vectors
     * @return average distance
     */
    private static double kmeansObj(JavaRDD<Vector> pointset, ArrayList<Vector> centers) {
        Long sizeOfP = pointset.count();
        Double sumOfDistances = pointset.map(x -> distance(x, centers)).reduce(Double::sum);
        return sumOfDistances/sizeOfP;
    }

    /**
     * Receives in input a set of points P and a set of centers C,
     * and returns the average distance of a point of P from C
     *
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

        ArrayList<Boolean> test123=new ArrayList<>();
        for(int i = 0; i < k; i++){
            test123.add(false);
        }

        for(Vector p : P){
            double minDistance = distance(p,S);
            int closestCenterIndex=-1;
            //lets find at which centers p belongs
            for(int i = 0; i < k; i++){
                double distance = Math.sqrt(Vectors.sqdist(p, S.get(i)));
                if(distance == minDistance){
                    closestCenterIndex = i;
                }
            }
            if (closestCenterIndex==-1){
                closestCenterIndex=0;
            }
            //the point P belongs to the cluster l
            clusters.get(closestCenterIndex).add(p);
            test123.set(closestCenterIndex,true);
            System.out.println(closestCenterIndex+" <- "+p);
        }
        System.out.println(test123);
        return clusters;
    }


    /**
     * Compute the distance between a point and the closest center.
     *
     * @param vector point for which we want to calculate the distance
     * @param S set of centers
     * @return minDistance the distance to the closest center
     */
    private static double distance(Vector vector, ArrayList<Vector> S) {
        double minDistance = Double.POSITIVE_INFINITY;
        for(Vector center: S){
            double distance = Math.sqrt(Vectors.sqdist(vector,center));
            if(distance < minDistance){
                minDistance = distance;
            }
        }
        return minDistance;
    }


}
