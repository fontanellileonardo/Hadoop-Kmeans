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
        //System.out.println("Punto: id: " + id + " Coordinate: " + point_sum.getVector()[0] + " " + point_sum.getVector()[1]);
        int final_how_many = 1;
        while(it.hasNext()){
            Point p = it.next();
            //System.out.println("Punto: id: " + id + " Coordinate: " + p.getVector()[0] + " " + p.getVector()[1]);
            point_sum.add(p);
            final_how_many++;
        } 
        point_sum.avg(final_how_many);
        //System.out.println("NUOVI CENTROIDI: " + point_sum.getVector()[0] + " " + point_sum.getVector()[1]);
        String temp = "";
        Text t = null;
        for(int i = 0; i < point_sum.getVector().length ; i++)
            temp += point_sum.getVector()[i] + " ";
        t = new Text(temp);
        context.write(id, t);
    }
}

