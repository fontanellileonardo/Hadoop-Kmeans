package it.unipi.hadoop;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KmeansReducer extends Reducer<Centroid, Point, IntWritable, Text>{

    public void reduce(Centroid id, Iterable<Point> points, Context context) throws IOException, InterruptedException{
        final Iterator<Point> it = points.iterator();
        Point point_sum = new Point(it.next());
        // Parse every point 
        while(it.hasNext()){
            Point p = new Point(it.next());
            // Add every coordinate of the points
            point_sum.add(p);
        }
        // Get the average of the point in order to get the new centroid 
        point_sum.avg();
        String temp = "";
        Text t = null;
        for(int i = 0; i < point_sum.getVector().length ; i++)
            temp += point_sum.getVector()[i] + " ";
        t = new Text(temp);
        context.write(id.getId(), t);
    }
}

