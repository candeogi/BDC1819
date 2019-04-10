# BDC1819
Repository for Big Data Computing Course
* [Homework 1](https://github.com/candeogi/BDC1819/blob/master/src/main/java/G32HM1.java)
* [Homework 2](https://github.com/candeogi/BDC1819/blob/master/src/main/java/G32HM2.java)
* ...
* ...
## Homeworks

### Homework 1

**Assignment**  
After doing the preliminary set up of your machine as explained above, create a program GxxHM1.java (for Java users) or GxxHM1.py (for Python users), where xx is your two-digit group number, which does the following things:
1. Reads an input file (dataset.txt) of nonnegative doubles into an RDD dNumbers, as in the template.
2. Computes and prints the maximum value in dNumbers. This must be done in two ways:
    * using the reduce method of the RDD interface;
    * using the max method of the RDD interface (For Java users, read here about some work-around require to pass a comparator to the method.) 
3. Creates a new RDD dNormalized containing the values of dNumbers normalized in [0,1].
4. Computes and prints a statistics of your choice on dNormalized. Make sure that you use at least one new method provided by the RDD interface. 

**Solution:** [Homework 1](https://github.com/candeogi/BDC1819/blob/master/src/main/java/G32HM1.java)  
___
### Homework 2
**Assigment**  
Create a program GxxHM2.java (for Java users) or GxxHM2.py (for Python users), where xx is your two-digit group number, which receives in input an integer K and a collection of documents, represented as a text file (one line per document) whose name is provided on the command line, and does the following things:

1. Reads the collection of documents into an RDD docs and subdivides the into K parts;
2. Runs the following MapReduce Word count algorithms and returns their individual running times, carefully measured:
    * the Improved Word count 1 algorithm described in class the using reduceByKey method to compute the final counts (you can modify the template provided above);
    * two variants of the Improved Word count 2 algorithm described in class, namely:
        * a variant of the algorithm presented in class where random keys take K possible values, where K is the value given in input. The easiest thing to do is to assing random integers between 0 and K-1. The algorithm presented in class had K=N^(1/2). You must use method groupBy to assign random keys to pairs and to group the pairs based on the assigned keys as required in the first round of the algorithm.

        * a variant that does not explicitly assign random keys but exploits the subdivision of docs into K parts in combination with mapPartitionToPair to access each partition separately. Again, K is the value given in input. Note that if docs was initially partitioned into K parts, then, even after transformations that act on individual elements, the resulting RDD stays partitioned into K parts and you may exploit this partition. However, you can also invoke repartition(K) to reshuffle the RDD elements at random. Do whatever you think it yields better performance. 
    
    It is important that both variants of Improved Word count 2 which you implement use the same number of parts K for the partition, where K is the value provided in input. 
3. Prints the average length of the distinct words appearing in the documents. 

Try to make each version as fast as possible. You can test you program using as input the file text-sample.txt, which contains 10122 documents from Wikipedia (one document per line) with 3503570 word occurrences overall. 

**Solution:** [Homework 2](https://github.com/candeogi/BDC1819/blob/master/src/main/java/G32HM2.java)