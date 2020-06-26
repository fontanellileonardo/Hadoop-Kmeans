package it.unipi.hadoop;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
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
    public HashMap<Integer,Point> readInputFile(final int[] id, final String inputFile, final int k){
        FileReader fr_input = null;
        HashMap<Integer,Point> coords = new HashMap<Integer,Point>();
        try {
            fr_input = new FileReader(inputFile);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        BufferedReader br = new BufferedReader(fr_input);
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
            fr_input.close();
            br.close();
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
                fs = outputPath.getFileSystem(conf);
                br = new BufferedReader(new InputStreamReader(fs.open(pt)));
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e){
                e.printStackTrace();
            }
            try {
                String line;
                String temp = "";
                // Counting number of lines of that reducer output file
                int lines = kmeans.countLines(conf, path);
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
            fs.close();
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return count;
    }

    // Generate random number to choose centroids from input file
    public int[] generateRandom(final int k, final int numberLines){
        int[] random = new int[k];
        int j = 0;
        boolean repeated = false;
        Random r = new Random();
        // Setting the seed
        r.setSeed(20);
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
    }

    public Job createJob(final Configuration conf, final String inputFile, final Path outputPath, final int numberLines){
        Job job = null;
        try {
            job = Job.getInstance(conf, "kmeans");
            job.setJarByClass(Kmeans.class);
            job.setMapperClass(KmeansMapper.class);
            // TODO Decidere le sorti del combiner
            //job.setCombinerClass(Combiner.class);
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(Point.class);
            /* TODO Da ridefinire
            File f = new File(inputFile);
            int split = ((int) f.length() / 3) + 1;
            System.out.println("Numero di split: " + split);
            conf.set("mapred.max.split.size", String.valueOf(split));
            */
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
        if (otherArgs.length < 3) {
            System.err.println("Usage: kmeans <input file> <k> <output file>");
            System.exit(2);
        }
        // Save the input file name
        final String inputFile = otherArgs[0];  
        // Save the output file path
        final Path outputPath = new Path(otherArgs[2]);
        // Save the number of cluster from the input 
        final int k = Integer.parseInt(otherArgs[1]);
        int[] r = new int[k];
        Kmeans kmeans = new Kmeans();
        // Save the number of lines of the input file;
        final int numberLines = kmeans.countLines(conf, inputFile);
        // Generate random number in order to randomly select the centroids
        r = kmeans.generateRandom(k, numberLines);
        // Get the corresponding coordinates in the inputFile   
        oldCoords = kmeans.readInputFile(r, inputFile, k);
        String[] coords = new String[k];
        for(Integer index : oldCoords.keySet()) {
            coords[index] = oldCoords.get(index).getCoords();
        }
        // Pass the coordinates of the centroid to the mapper with the number of clusters
        conf.setStrings("centroids", coords);
        conf.setInt("k", k);
        Job job = kmeans.createJob(conf, inputFile, outputPath, numberLines);
        int iterations = 0;
        while (iterations < Utils.MAX_ITERATIONS){
            if(iterations != 0){
                newCoords = kmeans.readFile(conf, outputPath, k);
                // Compute the shift
                double shift = 0.0;
                for(Integer index : newCoords.keySet()) 
                    shift += oldCoords.get(index).getDistance(newCoords.get(index));
                System.out.println("Shift: " + shift);
                // If the shift is less than a certain treshold, the program terminates and emits the result achieved at that iteration
                if(shift <= Utils.MIN_SHIFT) {
                    System.out.println("Min shift achieved");
                    break;
                }
                oldCoords = newCoords;
                String[] result = new String[k];
                for(Integer index : newCoords.keySet())
                    result[index] = newCoords.get(index).getCoords();
                // Pass the new coordinates of the centroids to the mapper for the next iteration
                conf.setStrings("centroids", result);
                job = kmeans.createJob(conf, inputFile, outputPath, numberLines);
            }
            if(job.waitForCompletion(true) == false){
                System.err.println("Job termined with error");
                System.exit(1);
            }
            iterations++;
        }
        System.out.println("Max iterations achieved");
        System.exit(0);
    }
}
