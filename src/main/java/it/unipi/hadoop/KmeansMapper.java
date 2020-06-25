package it.unipi.hadoop;

import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KmeansMapper extends Mapper<Object, Text, IntWritable, Point>{

    private Centroid[] coords;
    private int k;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException{
        super.setup(context);
        // Get the values of the centroids
        String[] lines = context.getConfiguration().getStrings("centroids");
        // Get the number of centroids, -1 default value 
        k = context.getConfiguration().getInt("k", -1);
        coords = new Centroid[k];
        // Get the values of the centroids
        for(int i = 0; i < lines.length; i++){
            String[] temp = lines[i].split(" ");
            double[] c = new double[temp.length];
            int count = 0;
            for (String x : temp) {
                c[count] = Double.parseDouble(x);
                count++;
            }
            coords[i] = new Centroid(i, c);
        }
    }

    public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException{
        // For each point, we associate the closest centroid
        //System.out.println("Valore di value: " + value.toString());
        Point p = new Point(value.toString());
        //System.out.println("Point preso in mapper: "+p.getVector()[0]+" "+p.getVector()[1]);
        double min_distance = 0.0;
        int id_min_distance = -1;
        for (int i = 0; i < k; i++){
            double distance = p.getDistance(coords[i].getPoint());
            if (i == 0 || min_distance > distance){
                min_distance = distance;
                id_min_distance = i;
            }
        }
        System.out.println("id nel mapper: "+id_min_distance);
        context.write(new IntWritable(id_min_distance), p);
    }
}
