package it.unipi.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.Writable;

public class Point implements Writable{

    private ArrayPrimitiveWritable coords = null;

    public Point() {
        coords = new ArrayPrimitiveWritable();
    }

    public Point(double[] array) {
        setVector(array);
    }

    public void setVector(double[] vector) {
        this.coords.set(vector);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        // TODO Auto-generated method stub

    }

}
