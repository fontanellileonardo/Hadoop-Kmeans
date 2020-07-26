# A MapReduce implementation of the K-Means Algorithm in Hadoop

## Introduction
In this repository you can find a MapReduce implementation of the K-Means algorithm. The code is written in Java since it has been developed for the Hadoop framework.

## Execute the code
In order to execute the code you need to compile the project with **Maven** and obtain a *.jar* file that will be uploaded in the Hadoop cluster. 
For executing the code in the cluster you must run the following command:
```sh
hadoop jar Hadoop-Kmeans-1.0-SNAPSHOT.jar it.unipi.hadoop.Kmeans <input file> <centroid file> <cluster number> <reducers number> <output folder>
```
The arguments are mandatory in order to start the computations. 

## Documentation
The documentation of the didactic project related to this repository is available [here](https://github.com/fontanellileonardo/Hadoop-Kmeans/blob/master/doc/CLOUD_Project_Hadoop___Spark_Documentation.pdf).
In the documentation it is also reported the results of the **Spark** framework execution, the related repository is available [here](https://github.com/fontanellileonardo/Spark-Kmeans).

## Credits
D.Comola, E. Petrangeli, G. Alvaro, L. Fontanelli
