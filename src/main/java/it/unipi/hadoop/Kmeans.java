package it.unipi.hadoop;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
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

    //Taking centroids coordinates from input file at first iteration 
    public HashMap<Integer,Point> readInputFile(final int[] id, final String inputFile, final int k){
        FileReader fr_input = null;
        //FileWriter fw_output = null;
        HashMap<Integer,Point> coords = new HashMap<Integer,Point>();
        try {
            fr_input = new FileReader(inputFile);
            //fw_output = new FileWriter(new File("First_Centroids.txt"), true);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e){
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
                //System.out.println("centroide preso dal file di input: "+line+" id: "+i);
                coords.put(i,new Point(line));
                //fw_output.append(i + " " + line + "\n");
                j++;
            }
            fr_input.close();
            br.close();
            //fw_output.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return coords;
    }

    //Taking centroids coordinates from output file from the second iteration onwards and delete output file 
    public HashMap<Integer,Point> readFile(Configuration conf, Path outputPath, int k){
        BufferedReader br = null;
        HashMap<Integer,Point> coords = new HashMap<Integer,Point>();
        FileSystem fs = null;
        Path pt = new Path(outputPath + "/" + Utils.FILE_NAME);
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
            for(int i = 0; i < k; i++){
                line = br.readLine();
                //System.out.println("Linea letta: " + line);
                String[] split = line.split("\\s+");
                for(int j = 1; j < split.length; j++)
                    temp += split[j] + " ";
                coords.put(Integer.parseInt(split[0]),new Point(temp));
                temp = "";
                //System.out.println("Valore di coords[" + i + "]: " + coords[i]);
            }
            //fs.delete(outputPath, true);
            br.close();
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return coords;
    }

    // Count the number of lines in the inputFile
    public int countLines(String inputFile){
        int count = 0;
        FileReader fr = null;
        try {
            fr = new FileReader(inputFile);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        BufferedReader br = new BufferedReader(fr);
        try {
            while (br.readLine() != null) 
                count++;
            fr.close();
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return count;
    }

    public int[] generateRandom(int k, String inputFile){
        int[] random = new int[k];
        int j = 0;
        boolean repeated = false;
        Random r = new Random();
        int generated;
        Kmeans kmeans = new Kmeans();
        // TODO: AGGIUNGERE SEED
        // Generate randomly centroids using input file
        while(j < k){
            // take a random number between 0 and "#lines in the input file"
            generated = r.nextInt(kmeans.countLines(inputFile));
            repeated = false;
            if (j == 0) {
                // first random number
                random[j] = generated;
                //System.out.println("CENTROIDE: " + random[j]);
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
                    //System.out.println("CENTROIDE: " + random[j]);
                    j++;
                }
            }
        }
        // Sort the random numbers
        Arrays.sort(random);

        return random;
    }

    public Job createJob(Configuration conf, String inputFile, Path outputPath){
        Job job = null;
        try {
            job = Job.getInstance(conf, "kmeans");
            job.setJarByClass(Kmeans.class);
            job.setMapperClass(KmeansMapper.class);
            //job.setCombinerClass(Combiner.class);
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(Point.class);
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
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 3) {
            System.err.println("Usage: kmeans <input file> <k> <output file>");
            System.exit(2);
        }
        final String inputFile = otherArgs[0];  
        final Path outputPath = new Path(otherArgs[2]);
        final int k = Integer.parseInt(otherArgs[1]);
        int[] r = new int[k];
        Kmeans kmeans = new Kmeans();
        r = kmeans.generateRandom(k, inputFile);
        // Get the corresponding coordinates in the inputFile   
        oldCoords = kmeans.readInputFile(r, inputFile, k);
        String[] coords = new String[k];
        for(Integer index : oldCoords.keySet()) {
            coords[index] = oldCoords.get(index).getCoords();
            //System.out.println("centroidi passati al mapper: "+coords[index]);
        }
        conf.setStrings("centroids", coords);
        conf.setInt("k", k);
        Job job = kmeans.createJob(conf, inputFile, outputPath);
        //TODO: aggiungere controllo sull'errore
        int iterations = 0;
        
        while (iterations < Utils.MAX_ITERATIONS){
            if(iterations != 0){
                //TODO: Sistemare se i reducer sono più di uno e stare attenti ad ordinare i valori delle coordinate 
                // in modo che siano sempre crescenti
                newCoords = kmeans.readFile(conf, outputPath, k);
                // Computer the error
                double error = 0.0;
                for(Integer index : newCoords.keySet()) 
                    error += oldCoords.get(index).getDistance(newCoords.get(index));
                System.out.println("Error: " + error);
                if(error <= Utils.MIN_ERROR) {
                    System.out.println("Min Error achieved");
                    break;
                }
                oldCoords = newCoords;
                String[] result = new String[k];
                for(Integer index : newCoords.keySet())
                    result[index] = newCoords.get(index).getCoords();
                conf.setStrings("centroids", result);
                job = kmeans.createJob(conf, inputFile, outputPath);
            }
            if(job.waitForCompletion(true) == false){
                System.err.println("Job termined with error");
                System.exit(1);
            }
            iterations++;
        }
        System.exit(0);
    }
}
