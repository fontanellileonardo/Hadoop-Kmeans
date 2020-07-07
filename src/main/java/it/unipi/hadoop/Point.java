package it.unipi.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;

public class Point implements Writable{

    private ArrayList<Double> coords = null;
    private int occurrences;

    public Point() {
        coords = new ArrayList<Double>();
        occurrences = 1;
    }

    public Point(String s){
        this();
        String[] c = s.split(" ");
        for (String x : c) {
            coords.add(Double.parseDouble(x));
        }
    }

    public Point(Point p){
        this();
        this.coords.addAll(p.coords);
        this.occurrences = p.occurrences;
    }

    public Point(double[] array){
        this();
        for (int i = 0; i < array.length; i++)
            coords.add(array[i]);
    }

    public void set(String s){
        String[] c = s.split(" ");
        coords.clear();
        for (String x : c) {
            coords.add(Double.parseDouble(x));
        }
    }

    public void set(Point p){
        this.coords = p.coords;
        this.occurrences = p.occurrences;
    }

    public int getNumber(){
        return occurrences;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(coords.size());
        for(int i = 0; i < coords.size(); i++)
            out.writeDouble(coords.get(i));
        out.writeInt(occurrences);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        coords = new ArrayList<Double>();
        for (int i = 0; i < size; i++){
            double e = in.readDouble();
            coords.add(e);
        }
        occurrences = in.readInt();
    }

    public void add(Point p){
        for (int i = 0; i < this.coords.size(); i++){
            this.coords.set(i, this.coords.get(i) + p.coords.get(i));
            System.out.print(this.coords.get(i) + " ");
        }
        this.occurrences += p.occurrences;
    }

    public void avg(){
        for(int i = 0; i < this.coords.size(); i++)
            this.coords.set(i, this.coords.get(i) / this.occurrences);
    }

    public double getDistance(Point p1){
        double sum = 0.0;
        for (int i = 0; i < p1.coords.size(); i++)
            sum += Math.pow(p1.coords.get(i) - this.coords.get(i), 2);
        return Math.sqrt(sum); 
    }

    @Override
    public String toString(){
        String temp = "";
        for (int i = 0; i < coords.size(); i++)
            temp += coords.get(i) + " ";
        return temp;
    }
}