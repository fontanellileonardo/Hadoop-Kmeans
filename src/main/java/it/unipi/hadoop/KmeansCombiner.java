package it.unipi.hadoop;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapreduce.Reducer;

public class KmeansCombiner extends Reducer<Centroid, Point, Centroid, Point> {

    public void reduce(Centroid id, Iterable<Point> points, Context context) throws IOException, InterruptedException{
        System.out.println("SONO NEL COMBINER!!!");
        final Iterator<Point> it = points.iterator();
        Point pointSum = new Point(it.next());
        System.out.println("[COMBINER]: Punto: " + pointSum.toString());
        // Parse every point 
        while(it.hasNext()){
            Point p = new Point(it.next());
            System.out.println("[COMBINER]: Punto: " + p.toString());
            // Add every coordinate of the points
            pointSum.add(p);
        }
        System.out.println("[COMBINER]: Centroide: " + id.getPoint().toString() + " Point: " + pointSum.toString() + " Occorrenze: " + pointSum.getNumber());
        context.write(id, pointSum);
    }
    
}