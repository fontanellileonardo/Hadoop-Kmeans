package it.unipi.hadoop;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class KmeansCombiner extends Reducer<IntWritable, Point, IntWritable, Point> {

    public void reduce(IntWritable id, Iterable<Point> points, Context context) throws IOException, InterruptedException{
        final Iterator<Point> it = points.iterator();
        Point point_sum = new Point(it.next());
        // Parse every point 
        while(it.hasNext()){
            Point p = new Point(it.next());
            // Add every coordinate of the points
            point_sum.add(p);
        }  
        context.write(id, point_sum);
    }
    
}