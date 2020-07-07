package it.unipi.hadoop;

public class Utils {
    // Maximum number of iterations after which the algorithm terminates
    protected final static int MAX_ITERATIONS = 10;
    // Minimum centroids shitf that, if achieved, the algorithm terminates
    protected final static double MIN_SHIFT = 0.05;
    // String of output file common to all the reducers
    protected final static String FILE_NAME = "part-r-000";
}