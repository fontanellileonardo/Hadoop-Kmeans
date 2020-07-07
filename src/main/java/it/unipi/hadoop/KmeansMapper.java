package it.unipi.hadoop;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KmeansMapper extends Mapper<Object, Text, Centroid, Point>{

    private final static ArrayList<Centroid> centroids = new ArrayList<Centroid>();
    private static int k;
    private final static Point p = new Point();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException{
        super.setup(context);
        String[] lines = context.getConfiguration().getStrings("centroids");
        // Get the number of centroids, -1 default value 
        k = context.getConfiguration().getInt("k", -1);
        // Get the values of the centroids
        for(int i = 0; i < lines.length; i++)
            centroids.add(new Centroid(i,lines[i]));
    }

    public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException{
        // For each point, we associate the closest centroid
        p.set(value.toString());
        double min_distance = 0.0;
        int id_min_distance = -1;
        // Computing euclidean distances from the target point and all the centroids to associate it
        for (int i = 0; i < k; i++){
            double distance = p.getDistance(centroids.get(i).getPoint());
            if (i == 0 || min_distance > distance){
                min_distance = distance;
                id_min_distance = i;
            }
        }
        context.write(centroids.get(id_min_distance), p);
    }
}
