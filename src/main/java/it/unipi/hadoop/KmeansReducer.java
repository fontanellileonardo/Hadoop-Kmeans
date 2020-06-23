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
        System.out.println("Punto: id: " + id + " Coordinate: " + point_sum.getVector()[0] + " " + point_sum.getVector()[1]);
        int final_how_many = 1;
        while(it.hasNext()){
            Point p = it.next();
            System.out.println("Punto: id: " + id + " Coordinate: " + p.getVector()[0] + " " + p.getVector()[1]);
            point_sum.add(p);
            final_how_many++;
        } 
        /*
        Point result = null;
        for (Point point : points) {
            if (result == null) {
                result = new Point(point);
            } else {
                result.add(point);
            }
        } 
        */
        point_sum.avg(final_how_many);
        //TODO Sistemare write con passaggio delle coordinate con un ciclo
        System.out.println("NUOVI CENTROIDI: " + point_sum.getVector()[0] + " " + point_sum.getVector()[1]);
        Text t = new Text(point_sum.getVector()[0] + " " + point_sum.getVector()[1]);
        context.write(id, t);
    }
}

