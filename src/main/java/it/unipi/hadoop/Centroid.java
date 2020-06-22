package it.unipi.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class Centroid implements WritableComparable<Centroid>{

    private IntWritable id;
    private Point point;

    public Centroid(){
        id = new IntWritable(0);
        point = new Point();
    }

    public Centroid(int id, double[] coords) {
        this.id = new IntWritable(id);
        for(double d : coords)
            System.out.println("Array coords: " + d);
        this.point = new Point(coords);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public int compareTo(Centroid o) {
        // TODO Auto-generated method stub
        return 0;
    }

    public Point getPoint(){
        return point;
    }
}
