package it.unipi.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class Centroid implements WritableComparable<Centroid>{

    private IntWritable id;
    private Point point;

    public Centroid() {
        id = new IntWritable(0);
        point = new Point();
    }

    public Centroid(int id, String s) {
        this.id = new IntWritable(id);
        this.point = new Point(s);
    }

    public IntWritable getId(){
        return this.id;
    }

    public Point getPoint() {
        return point;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        id.write(out);
        point.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        id.readFields(in);
        point.readFields(in);
    }

    @Override
    public int compareTo(Centroid o) {
        return this.id.compareTo(o.getId());
    }
}
