### Homework 4
**Assignment**

This homework will show how the coreset-based approach enables an effective and efficient solution to the k-median clustering problem. 
In particular, we will test the MapReduce algorithm MR-kmedian seen in class using the already implemented implementation of kmeans++ devised for Homework 3 as a primitive. 

After developing and debugging your implementation locally, you will test it on CloudVeneto, a cloud infrastructure at UNIPD. On the cloud you have access to a cluster of 10 machines, each equipped with 8 cores and 16 GB of RAM. Of these 10 machines, 9 are devoted to execute parallel Spark tasks, and 1, called frontend, is responsible of coordinating jobs and managing resources. 