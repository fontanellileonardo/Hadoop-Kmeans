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

    public Point(String array) {
        String[] c = array.split(" ");
        double[] d = new double[c.length];
        int k = 0;
        for (String x : c) {
            d[k] = Double.parseDouble(x);
            k++;
        }
        setVector(d);
    }

    public Point(double[] array){
        setVector(array);
    }

    public void setVector(double[] vector) {
        this.coords.set(vector);
    }

    public double[] getVector(){
        return (double[]) coords.get();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        // TODO Auto-generated method stub

    }

    public void add(Point p){
        double[] c = getVector();
        double[] points = p.getVector();
        for (int i = 0; i < c.length; i++)
            c[i] += points[i];
        this.setVector(c);
    }

    public void avg(int sum){
        double[] c = getVector(); 
        for (int i = 0; i < c.length; i++)
           c[i] = c[i] / sum;
        this.setVector(c); 
    }
}