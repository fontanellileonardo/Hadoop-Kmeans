package it.unipi.hadoop;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Random;

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

    // Taking centroids coordinates from input file at first iteration 
    public HashMap<Integer,Point> readInputFile(final Configuration conf, final int[] id, final Path inputPath, final int k){
        FileSystem fs_input = null;
        BufferedReader br = null;
        HashMap<Integer,Point> coords = new HashMap<Integer,Point>();
        try {
            fs_input = inputPath.getFileSystem(conf);
            br = new BufferedReader(new InputStreamReader(fs_input.open(inputPath)));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e){
            e.printStackTrace();
        }
        int j = 0;
        try {
            for (int i = 0; i < id.length; i++) {
                String line = null;
                while (j < id[i]) {
                    br.readLine();
                    j++;
                }
                line = br.readLine();
                coords.put(i,new Point(line));
                j++;
            }
            br.close();
            fs_input.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return coords;
    }

    //Taking centroids coordinates from output file from the second iteration onwards and delete output file 
    public HashMap<Integer,Point> readFile(final Configuration conf, final Path outputPath, final int k){
        BufferedReader br = null;
        HashMap<Integer,Point> coords = new HashMap<Integer,Point>();
        FileSystem fs = null;
        Kmeans kmeans = new Kmeans();
        // Merging output files from multiple reducers
        for(int fileNumber = 0; fileNumber < Utils.NUM_REDUCERS; fileNumber++ ){
            String name = "";
            if(fileNumber < 10)
                name = "0" + fileNumber;
            else 
                name = String.valueOf(fileNumber);
            String path = outputPath + "/" + Utils.FILE_NAME + name;
            Path pt = new Path(path);
            try {
                // Counting number of lines of that reducer output file
                int lines = kmeans.countLines(conf, path);
                fs = outputPath.getFileSystem(conf);
                br = new BufferedReader(new InputStreamReader(fs.open(pt)));
                String line;
                String temp = "";
                for(int i = 0; i < lines; i++){
                    line = br.readLine();
                    String[] split = line.split("\\s+");
                    for(int j = 1; j < split.length; j++)
                        temp += split[j] + " ";
                    coords.put(Integer.parseInt(split[0]),new Point(temp));
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
        return coords;
    }

    // Count the number of lines of the file
    public int countLines(final Configuration conf, final String inputFile){
        int count = 0;
        FileSystem fs = null;
        Path pt = new Path(inputFile);
        BufferedReader br = null;
        try {
            fs = pt.getFileSystem(conf);
            br = new BufferedReader(new InputStreamReader(fs.open(pt)));
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            while (br.readLine() != null) 
                count++;
            br.close();
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return count;
    }

    // Generate random number to choose centroids from input file
    public int[] generateRandom(final int k, final int numberLines, final int seed){
        int[] random = new int[k];
        int j = 0;
        boolean repeated = false;
        Random r = new Random();
        // Setting the seed
        r.setSeed(seed);
        int generated;
        // Generate randomly centroids using input file
        while(j < k){
            // take a random number between 0 and "#lines in the input file"
            generated = r.nextInt(numberLines);
            repeated = false;
            if (j == 0) {
                // first random number
                random[j] = generated;
                j++;
            }
            else {
                for (int i = 0; i < j; i++) {
                    // check if the random number was already generated
                    if (generated == random[i]) {
                        repeated = true;
                    }
                }
                if(repeated == false) {
                    // if the random number generated is a new one -> the random number is saved
                    random[j] = generated;
                    j++;
                }
            }
        }
        // Sort the random numbers
        Arrays.sort(random);
        return random;
        /*
        int[] random = new int[k];
        for (int i = 0; i < k ; i++){
            random[i] = i;
            System.out.println("Centroidi scelti: " + random[i]);
        }
        return random;
        */
    }

    public Job createJob(final Configuration conf, final String inputFile, final Path outputPath, final int numberLines){
        Job job = null;
        try {
            job = Job.getInstance(conf, "kmeans");
            job.setJarByClass(Kmeans.class);
            job.setMapperClass(KmeansMapper.class);
            job.setCombinerClass(KmeansCombiner.class);
            job.setMapOutputKeyClass(Centroid.class);
            job.setMapOutputValueClass(Point.class);
            job.setNumReduceTasks(Utils.NUM_REDUCERS);
            job.setReducerClass(KmeansReducer.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(inputFile));
            outputPath.getFileSystem(conf).delete(outputPath, true);
            FileOutputFormat.setOutputPath(job, outputPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return job;
    }

    public static void main(String[] args) throws Exception{
        HashMap<Integer,Point> oldCoords = new HashMap<Integer,Point>();
        HashMap<Integer,Point> newCoords = new HashMap<Integer,Point>();
        Configuration conf = new Configuration();
        // Parse the input line for retrieving the arguments passed
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 4) {
            System.err.println("Usage: kmeans <input file> <k> <seed> <output folder>");
            System.exit(2);
        }
        // Save the input file name
        final String inputFile = otherArgs[0];  
        final Path inputPath = new Path(inputFile);
        // Save the output file path
        final Path outputPath = new Path(otherArgs[3]);
        // Save the number of cluster from the input 
        final int k = Integer.parseInt(otherArgs[1]);
        // Save the seed of the random function from the input
        final int seed = Integer.parseInt(otherArgs[2]);
        int[] r = new int[k];
        Kmeans kmeans = new Kmeans();
        // Save the number of lines of the input file;
        final int numberLines = kmeans.countLines(conf, inputFile);
        // Generate random number in order to randomly select the centroids
        r = kmeans.generateRandom(k, numberLines, seed);
        // Get the corresponding coordinates in the inputFile   
        oldCoords = kmeans.readInputFile(conf, r, inputPath, k);
        String[] coords = new String[k];
        for(Integer index : oldCoords.keySet()) {
            coords[index] = oldCoords.get(index).getCoords();
        }
        // Pass the coordinates of the centroid to the mapper with the number of clusters
        conf.setStrings("centroids", coords);
        conf.setInt("k", k);
        int iterations = 0;
        double shift = 0.0;
        while (iterations < Utils.MAX_ITERATIONS){
            Job job = kmeans.createJob(conf, inputFile, outputPath, numberLines);
            if(job.waitForCompletion(true) == false){
                System.err.println("Job termined with error");
                System.exit(1);
            }
            shift = 0.0;
            newCoords = kmeans.readFile(conf, outputPath, k);
            // Compute the shift
            for(Integer index : newCoords.keySet()) 
                shift += oldCoords.get(index).getDistance(newCoords.get(index));
            System.out.println("Iteration number: " + iterations + " Shift: " + shift);
            // If the shift is less than a certain treshold, the program terminates and emits the result achieved at that iteration
            if(shift <= Utils.MIN_SHIFT) {
                System.out.println("Min shift achieved");
                break;
            }
            oldCoords = newCoords;
            for(Integer index : newCoords.keySet())
                System.out.println("Centroide " + index + ": " + newCoords.get(index).getCoords());
            // Retrieves the coordinates of the new centroids and iterates to copy the new coordinates in the array
            String[] result = new String[k];
            for(Integer index : newCoords.keySet())
                result[index] = newCoords.get(index).getCoords();
            // Pass the new coordinates of the centroids to the mapper for the next iteration
            conf.setStrings("centroids", result);      
            iterations++;
        }
        if(iterations == Utils.MAX_ITERATIONS)
            System.out.println("Max iterations achieved");
        System.exit(0);
    }
}
