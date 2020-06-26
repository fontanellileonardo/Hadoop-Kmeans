package it.unipi.hadoop;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KmeansReducer extends Reducer<IntWritable, Point, IntWritable, Text>{

    public void reduce(IntWritable id, Iterable<Point> points, Context context) throws IOException, InterruptedException{
        final Iterator<Point> it = points.iterator();
        Point point_sum = new Point(it.next());
        int final_how_many = 1;
        // Parse every point 
        while(it.hasNext()){
            Point p = it.next();
            // Add every coordinate of the points
            point_sum.add(p);
            // Incrementing number of occurrences
            final_how_many++;
        }
        // Get the average of the point in order to get the new centroid 
        point_sum.avg(final_how_many);
        String temp = "";
        Text t = null;
        for(int i = 0; i < point_sum.getVector().length ; i++)
            temp += point_sum.getVector()[i] + " ";
        t = new Text(temp);
        context.write(id, t);
    }
}

