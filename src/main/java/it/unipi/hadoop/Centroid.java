package it.unipi.hadoop;

import org.apache.hadoop.io.IntWritable;

public class Centroid{

    private IntWritable id;
    private Point point;

    public Centroid(){
        id = new IntWritable(0);
        point = new Point();
    }

    public Centroid(int id, double[] coords) {
        this.id = new IntWritable(id);
        this.point = new Point(coords);
    }

    public Point getPoint(){
        return point;
    }
}
