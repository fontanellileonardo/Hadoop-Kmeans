package it.unipi.hadoop;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Kmeans {

    private final static HashMap<Integer,Point> centroids = new HashMap<Integer,Point>();

    // Taking centroids coordinates from input file at first iteration 
    public static void readCentroidsFile(final Configuration conf, final Path inputPath) {
        FileSystem fs_input = null;
        BufferedReader br = null;
        try {
            fs_input = inputPath.getFileSystem(conf);
            br = new BufferedReader(new InputStreamReader(fs_input.open(inputPath)));
            int i = 0;
            String line = null;
            while((line = br.readLine()) != null) {
                centroids.put(i ,new Point(line));
                i++;
            }
            br.close();
            fs_input.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //Taking centroids coordinates from output file from the second iteration onwards and delete output file 
    public static double readOutputFile(final Configuration conf, final Path outputPath, final int k, final int numReducers){
        BufferedReader br = null;
        FileSystem fs = null;
        double shift = 0.0;
        Point newCoords = new Point();
        // Merging output files from multiple reducers
        for(int fileNumber = 0; fileNumber < numReducers; fileNumber++ ){
            String name = "";
            if(fileNumber < 10)
                name = "0" + fileNumber;
            else 
                name = String.valueOf(fileNumber);
            String path = outputPath + "/" + Utils.FILE_NAME + name;
            Path pt = new Path(path);
            try {
                // The lines of the output files are inspected and the shift for the new centroids is computed
                fs = outputPath.getFileSystem(conf);
                br = new BufferedReader(new InputStreamReader(fs.open(pt)));
                String line;
                String temp = "";
                while((line = br.readLine()) != null){ 
                    String[] split = line.split("\\s+");
                    for(int j = 1; j < split.length; j++)
                        temp += split[j] + " ";
                    newCoords.set(temp);
                    int index = Integer.parseInt(split[0]);
                    shift += centroids.get(index).getDistance(newCoords);
                    centroids.put(index, new Point(newCoords));
                    temp = "";
                }
                br.close();
                fs.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } 
        }
        return shift;
    }

    public static Job createJob(final Configuration conf, final Path inputPath, final Path outputPath, final int numReducers){
        Job job = null;
        try {
            job = Job.getInstance(conf, "kmeans");
            job.setJarByClass(Kmeans.class);
            job.setMapperClass(KmeansMapper.class);
            job.setCombinerClass(KmeansCombiner.class);
            job.setMapOutputKeyClass(Centroid.class);
            job.setMapOutputValueClass(Point.class);
            job.setNumReduceTasks(numReducers);
            job.setReducerClass(KmeansReducer.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, inputPath);
            outputPath.getFileSystem(conf).delete(outputPath, true);
            FileOutputFormat.setOutputPath(job, outputPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return job;
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        // Parse the input line for retrieving the arguments passed
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 5) {
            System.err.println("Usage: kmeans <input file> <centroid file> <k> <reducers> <output folder>");
            System.exit(2);
        }
        // Save the input files name
        final Path inputPath = new Path(otherArgs[0]);
        final Path centroidPath = new Path(otherArgs[1]);
        // Save the output file path
        final Path outputPath = new Path(otherArgs[4]);
        // Save the number of cluster from the input 
        final int k = Integer.parseInt(otherArgs[2]);
        // Save the number of reducers, if it is greater than the number of cluster it will be set to the number of cluster
        final int numReducers = (Integer.parseInt(otherArgs[3]) > k) ? k : Integer.parseInt(otherArgs[3]);        
        // Read the centroids from the the centroid File given as input for the first iteration
        readCentroidsFile(conf, centroidPath);
        String[] coords = new String[k];
        for(Integer index : centroids.keySet()) {
            coords[index] = centroids.get(index).toString();
        }
        // Pass the coordinates of the centroid to the mapper with the number of clusters
        conf.setStrings("centroids", coords);
        conf.setInt("k", k);
        int iterations = 0;
        while (iterations < Utils.MAX_ITERATIONS){
            Job job = createJob(conf, inputPath, outputPath, numReducers);
            if(job.waitForCompletion(true) == false){
                System.err.println("Job termined with error");
                System.exit(1);
            }
            double shift = readOutputFile(conf, outputPath, k, numReducers);
            System.out.println("Iteration number: " + iterations + " Shift: " + shift);
            // If the shift is less than a certain treshold, the program terminates and emits the result achieved at that iteration
            if(shift <= Utils.MIN_SHIFT) {
                System.out.println("Min shift achieved");
                break;
            }
            for(Integer index : centroids.keySet())
                System.out.println("Centroids " + index + ": " + centroids.get(index).toString());
            // Retrieves the coordinates of the new centroids and iterates to copy the new coordinates in the array
            String[] result = new String[k];
            for(Integer index : centroids.keySet())
                result[index] = centroids.get(index).toString();
            // Pass the new coordinates of the centroids to the mapper for the next iteration
            conf.setStrings("centroids", result);      
            iterations++;
        }
        if(iterations == Utils.MAX_ITERATIONS)
            System.out.println("Max iterations achieved");
        System.exit(0);
    }
}
