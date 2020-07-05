package it.unipi.hadoop;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KmeansReducer extends Reducer<Centroid, Point, IntWritable, Text>{

    public void reduce(Centroid id, Iterable<Point> points, Context context) throws IOException, InterruptedException{
        System.out.println("SONO NEL REDUCER!!!!!");
        final Iterator<Point> it = points.iterator();
        Point pointSum = new Point(it.next());
        System.out.println("[REDUCER]: Punto: " + pointSum.toString());
        // Parse every point 
        while(it.hasNext()){
            Point p = new Point(it.next());
            System.out.println("[REDUCER]: Punto: " + p.toString());
            // Add every coordinate of the points
            pointSum.add(p);
        }
        // Get the average of the point in order to get the new centroid 
        pointSum.avg();
        // String temp = "";
        Text t = new Text(pointSum.getStringCoords());
        /*
        for(int i = 0; i < point_sum.getVector().length ; i++)
            temp += point_sum.getVector()[i] + " ";
        t = new Text(temp);
        */
        System.out.println("[REDUCER]: Centroide: " + id.getPoint().toString() + " Point: " + t);
        context.write(id.getId(), t);
    }
}

