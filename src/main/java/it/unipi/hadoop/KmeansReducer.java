package it.unipi.hadoop;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KmeansReducer extends Reducer<Centroid, Point, IntWritable, Text>{

    private final static Point pointSum = new Point();
    private final static Point p = new Point();

    public void reduce(Centroid id, Iterable<Point> points, Context context) throws IOException, InterruptedException{
        final Iterator<Point> it = points.iterator();
        pointSum.set(it.next());
        // Parse every point 
        while(it.hasNext()){
            p.set(it.next());
            // Add every coordinate of the points
            pointSum.add(p);
        }
        // Get the average of the point in order to get the new centroid 
        pointSum.avg();
        Text t = new Text(pointSum.toString());
        context.write(id.getId(), t);
    }
}

