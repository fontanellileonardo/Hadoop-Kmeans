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
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Kmeans {

    public void readLine(int[] id) {
        FileReader fr = null;
        try {
            fr = new FileReader("file.txt");
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
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
                String[] temp = line.split(" ");
                double[] coords = new double[temp.length];
                int k = 0;
                for (String x : temp) {
                    coords[k] = Double.parseDouble(x);
                    k++;
                }
                j++;
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public int countLines() {
        int count = 0;
        FileReader fr = null;
        try {
            fr = new FileReader("file.txt");
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        BufferedReader br = new BufferedReader(fr);
        String line = null;
        try {
            line = br.readLine();
        } catch (IOException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        while (line != null)
            count++;
        try {
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
        if (otherArgs.length < 2) { //controllare numero parametri passati
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "kmeans");
        job.setJarByClass(Kmeans.class);
        job.setMapperClass(KmeansMapper.class);
        //job.setCombinerClass(Combiner.class);
        job.setReducerClass(KmeansReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //da definire in base agli input e output che abbiamo
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));

        int[] random = new int[Utils.K];

        int j = 0;
        boolean repeated = false;
        Random r = new Random();
        Kmeans k = new Kmeans();
        int generated;

        while(j < Utils.K) {
            generated = r.nextInt(k.countLines());
            repeated = false;
            if (j == 0) {
                random[j] = generated;
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
                    j++;
                }
            }
        }

        Arrays.sort(random);


        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
