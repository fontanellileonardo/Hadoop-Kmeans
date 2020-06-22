package it.unipi.hadoop;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class KmeansReducer extends Reducer<IntWritable, Point, IntWritable, Point>{

    public void reduce(final IntWritable id, final Iterable<Point> points, Context context) throws IOException, InterruptedException{
        final Iterator<Point> it = points.iterator();
        Point point_sum = new Point(it.next());
        System.out.println(point_sum.toString());
        int final_how_many = 1;
        while(it.hasNext()){
            point_sum.add(it.next());
            final_how_many++;
        }
        point_sum.avg(final_how_many);
        context.write(id, point_sum);
    }

}

