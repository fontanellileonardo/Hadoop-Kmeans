package it.unipi.hadoop;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapreduce.Reducer;

public class KmeansCombiner extends Reducer<Centroid, Point, Centroid, Point> {

    private final static Point pointSum = new Point();

    public void reduce(Centroid id, Iterable<Point> points, Context context) throws IOException, InterruptedException{
        final Iterator<Point> it = points.iterator();
        pointSum.set(it.next());
        // Parse every point 
        while(it.hasNext()){
            // Add every coordinate of the points
            pointSum.add(it.next());
        }
        context.write(id, pointSum);
    }
    
}