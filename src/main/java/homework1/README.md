### Homework 1

**Assignment**  
Create a program GxxHM1.java (for Java users) or GxxHM1.py (for Python users), where xx is your two-digit group number, which does the following things:
1. Reads an input file (dataset.txt) of nonnegative doubles into an RDD dNumbers, as in the template.
2. Computes and prints the maximum value in dNumbers. This must be done in two ways:
    * using the reduce method of the RDD interface;
    * using the max method of the RDD interface (For Java users, read here about some work-around require to pass a comparator to the method.) 
3. Creates a new RDD dNormalized containing the values of dNumbers normalized in [0,1].
4. Computes and prints a statistics of your choice on dNormalized. Make sure that you use at least one new method provided by the RDD interface. 