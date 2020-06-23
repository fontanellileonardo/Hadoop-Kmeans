package it.unipi.hadoop;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
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

    public String[] readLine(final int[] id, final int k, String inputFile) {
        FileReader fr = null;
        String[] coords = new String[k];
        try {
            fr = new FileReader(inputFile);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        BufferedReader br = new BufferedReader(fr);
        int j = 0;
        try {
            for (int i = 0; i < id.length; i++) {
                String line = null;
                while (j < id[i]) {
                    br.readLine();
                    j++;
                }
                line = br.readLine();
                coords[i] = line;
                j++;
            }
            fr.close();
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return coords;
    }

    public String[] readFile (Configuration conf, Path outputPath, int k){
        BufferedReader br = null;
        String[] coords = new String[k];
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
                System.out.println("Linea letta: " + line);
                String[] split = line.split("\\s+");
                for(int j = 1; j < split.length; j++)
                    temp += split[j] + " ";
                coords[i] = temp;
                temp = "";
                System.out.println("Valore di coords[i]: " + coords[i]);
            }
            fs.delete(outputPath, true);
            br.close();
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return coords;
    }

    public int countLines(String inputFile) {
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
            FileOutputFormat.setOutputPath(job, outputPath);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return job;
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 3) {
            System.err.println("Usage: kmeans <input file> <k> <output file>");
            System.exit(2);
        }
        final String inputFile = otherArgs[0];
        final Path outputPath = new Path(otherArgs[2]);
        final int k = Integer.parseInt(otherArgs[1]);
        int[] random = new int[k];
        int j = 0;
        boolean repeated = false;
        Random r = new Random();
        Kmeans kmeans = new Kmeans();
        int generated;
        while(j < k){
            generated = r.nextInt(kmeans.countLines(inputFile));
            repeated = false;
            if (j == 0) {
                random[j] = generated;
                //System.out.println("CENTROIDE: " + random[j]);
                j++;
            }
            else {
                for (int i = 0; i < j; i++) {
                    if (generated == random[i]) {
                        repeated = true;
                    }
                }
                if(repeated == false) {
                    random[j] = generated;
                    //System.out.println("CENTROIDE: " + random[j]);
                    j++;
                }
            }
        }
        Arrays.sort(random);
        String[] coords = kmeans.readLine(random, k, inputFile);
        conf.setStrings("centroids", coords);
        conf.setInt("k", k);
        Job job = kmeans.createJob(conf, inputFile, outputPath);
        //TODO: Aggiungere iterazioni e controllo completamento
        int iterations = 0;
        while (iterations < Utils.MAX_ITERATIONS){
            if(iterations != 0){
                String[] result = kmeans.readFile(conf, outputPath, k);
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
