package it.unipi.hadoop;

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
        String[] lines = context.getConfiguration().getStrings("centroids");
        k = context.getConfiguration().getInt("k", -1);
        System.out.print("Array: " + lines);
        for(int i = 0; i < lines.length; i++){
            String[] temp = lines[i].split(" ");
            double[] c = new double[temp.length];
            int k = 0;
            for (String x : temp) {
                c[k] = Double.parseDouble(x);
                k++;
            }
            coords[i] = new Centroid(i, c);
        }
    }

    public double getDistance(Point p1, Point p2){
        double[] p1Vector = p1.getVector();
        double[] p2Vector = p2.getVector();
        //if (p1Vector.length != p2Vector.length) throw new Exception("Invalid length");
        double sum = 0.0;
        for (int i = 0; i < p1Vector.length; i++){
            sum += Math.pow(p1Vector[i] - p2Vector[i], 2);
        }
        return Math.sqrt(sum);
    }

    public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException{
        // For each point, we associate the closest centroid
        Point p = new Point(value.toString());
        double min_distance = 0.0;
        int id_min_distance = -1;
        for (int i = 0; i < k; i++){
            double distance = getDistance(p, coords[i].getPoint());
            if (i == 0 || min_distance > distance){
                min_distance = distance;
                id_min_distance = i;
            }
        }
        context.write(new IntWritable(id_min_distance), p);
    }
}
