package it.unipi.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KmeansMapper extends Mapper<Object, Text, Centroid, Point>{

    private Centroid[] centroids;
    private int k;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException{
        super.setup(context);
        String[] lines = context.getConfiguration().getStrings("centroids");
        // Get the number of centroids, -1 default value 
        k = context.getConfiguration().getInt("k", -1);
        centroids = new Centroid[k];
        // Get the values of the centroids
        for(int i = 0; i < lines.length; i++){
            String[] temp = lines[i].split(" ");
            double[] c = new double[temp.length];
            int count = 0;
            for (String x : temp) {
                c[count] = Double.parseDouble(x);
                count++;
            }
            centroids[i] = new Centroid(i, c);
        }
    }

    public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException{
        // For each point, we associate the closest centroid
        Point p = new Point(value.toString());
        double min_distance = 0.0;
        int id_min_distance = -1;
        // Computing euclidean distances from the target point and all the centroids to associate it
        for (int i = 0; i < k; i++){
            double distance = p.getDistance(centroids[i].getPoint());
            if (i == 0 || min_distance > distance){
                min_distance = distance;
                id_min_distance = i;
            }
        }
        context.write(centroids[id_min_distance], p);
    }
}
