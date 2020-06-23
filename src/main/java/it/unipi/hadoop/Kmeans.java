package it.unipi.hadoop;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Kmeans {

    public String[] readLine(final int[] id, final int k) {
        FileReader fr = null;
        String[] coords = new String[k];
        try {
            fr = new FileReader("file.txt");
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
        } catch (IOException e) {
            e.printStackTrace();
        }
        return coords;
    }

    public int countLines() {
        int count = 0;
        FileReader fr = null;
        try {
            fr = new FileReader("file.txt");
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
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return count;
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 3) { //TODO controllare numero parametri passati
            System.err.println("Usage: kmeans <input file> <k> <output file>");
            System.exit(2);
        }
        final int k = Integer.parseInt(otherArgs[1]);
        int[] random = new int[k];
        int j = 0;
        boolean repeated = false;
        Random r = new Random();
        Kmeans kmeans = new Kmeans();
        int generated;
        while(j < k) {
            generated = r.nextInt(kmeans.countLines());
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
        String[] coords = kmeans.readLine(random, k);
        conf.setStrings("centroids", coords);
        conf.setInt("k", k);
        Job job = Job.getInstance(conf, "kmeans");
        job.setJarByClass(Kmeans.class);
        job.setMapperClass(KmeansMapper.class);
        //job.setCombinerClass(Combiner.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Point.class);
        job.setReducerClass(KmeansReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
        //TODO: Aggiungere iterazioni e controllo completamento
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
