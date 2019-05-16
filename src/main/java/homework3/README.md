### Homework 3
**Assignment**

For this homework you will not use RDDs! Instead, you must develop the following 2 methods (functions in Python):
1. Method kmeansPP(P,WP,k,iter): receives in input a set of points P, a set of weigths WP for P, and two integers "k" and "iter", and returns a set C of k centers computed as follows:
    * Compute a first set C' of centers using the weighted variant of the kmeans++ algorithm. In each iteration, the probability for a non-center point p of being chosen as next center is: w_p*(d_p)/(sum_{q non center} w_q*(d_q)), where d_p is the distance of p from the closest among the already selected centers and w_p is the weight of p. (Note that we are using distances and not square distances!). Make sure that the implementation of kmeans++ runs in time O(|P|*k). To do so, at every iteration you must maintain for each point q in P its distance from the closest among the current centers.
    * Compute the final C by refining C' using "iter" iterations of Lloyds' algorithm. Specifically, iteration i (with i=1,2, .. "iter"), computes a new set of centers C(i) as the centroids of the clusters obtained by assigning each point to the closest center of C(i-1). Hence, you start from C'=C(0) and return C=C("iter"). (Note that C will not necessarily be a subset of P).

2. Method kmeansObj(P,C): receives in input a set of points P and a set of centers C, and returns the average distance of a point of P from C (i.e., sum of the distances of the points from their closest centers divided by the number of points).
    *Java users should represent P, C and C' as instances of ArrayList<Vector>, and WP as instance of ArrayList<Long>. Python users should represent P, C and C' as lists of Vector and WP as list of intergers.

Create a program GxxHM3.java (for Java users) of GxxHM3.py (for Python users), where xx is your two-digit group number, which receives in input the name of a file (filename) containing a set P of points in Euclidean space, and 2 integers "k" and "iter". The file must contain one point per line with coordinates separated by spaces. The program incorporates the methods developed above and does the following:
* (for Java users) Reads the input points from the file into an ArrayList<Vector>. For reading the points you can use the method readVectorsSeq provided in the file VectorInput.java. (You will need the method strToVector and the import statements as well.)

Runs kmeansPP(P,WP,k,iter) with all weights in WP equal to 1, to obtain a set of k centers C, and then runs kmeansObj(P,C) printing the returned value. As a test data set you can download the file covtype.data which contains 10,000 points in 55-dimensional Euclidean space which is a sample of a larger dataset containing observations from a forest in Colorado. 
