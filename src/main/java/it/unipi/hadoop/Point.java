package it.unipi.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class Point implements Writable{

    private ArrayPrimitiveWritable coords = null;
    private IntWritable occurrences;

    public Point() {
        coords = new ArrayPrimitiveWritable();
        occurrences = new IntWritable(1);
    }

    public Point(String array) {
        this();
        String[] c = array.split(" ");
        double[] d = new double[c.length];
        int k = 0;
        for (String x : c) {
            d[k] = Double.parseDouble(x);
            k++;
        }
        setVector(Arrays.copyOf(d, d.length));
    }

    public Point(Point p){
        this();
        double[] vector = p.getVector();
        this.occurrences.set(p.occurrences.get());
        setVector(Arrays.copyOf(vector, vector.length));
    }

    public Point(double[] array){
        this();
        setVector(Arrays.copyOf(array, array.length));
    }

    public void setVector(double[] vector) {
        this.coords.set(vector);
    }

    public double[] getVector(){
        return (double[]) coords.get();
    }

    public int getNumber(){
        return this.occurrences.get();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        coords.write(out);
        occurrences.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        coords.readFields(in);
        occurrences.readFields(in);
    }

    public void add(Point p){
        double[] c = this.getVector();
        double[] points = p.getVector();
        for (int i = 0; i < c.length; i++)
            c[i] += points[i];
        this.occurrences.set(this.occurrences.get() + p.getNumber());
        this.setVector(Arrays.copyOf(c, c.length));
    }

    public void avg(){
        double[] c = this.getVector();
        for (int i = 0; i < c.length; i++)
           c[i] = c[i] / this.occurrences.get();
        this.setVector(Arrays.copyOf(c, c.length));
    }

    public String getCoords(){
        String coord = "";
        for(int i = 0; i < getVector().length; i++)
            coord += getVector()[i] + " ";
        return coord;
    }

    public double getDistance(Point p1){
        double[] p1Vector = p1.getVector();
        double[] p2Vector = this.getVector();
        double sum = 0.0;
        for (int i = 0; i < p1Vector.length; i++){
            sum += Math.pow(p1Vector[i] - p2Vector[i], 2);
        }
        return Math.sqrt(sum); 
    }
}
