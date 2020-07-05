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

    public Point(String array) {
        this();
        String[] c = array.split(" ");
        for (String x : c) {
            //System.out.println("Valore inserito in coords nel costruttore di Point: " + Double.parseDouble(x));
            coords.add(Double.parseDouble(x));
        }
    }

    public Point(Point p){
        //double[] vector = p.getVector();
        this.coords = p.coords;
        this.occurrences = p.occurrences;
        //setVector(Arrays.copyOf(vector, vector.length));
    }

    public Point(double[] array){
        this();
        for (int i = 0; i < array.length; i++)
            coords.add(array[i]);
    }
    
    /*
    public void setVector(double[] vector) {
        this.coords.set(vector);
    }

    public double[] getVector(){
        return (double[]) coords.get();
    }
    */

    public int getNumber(){
        return occurrences;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        //System.out.println("Size dell'array nella write: " + coords.size());
        out.writeInt(coords.size());
        //System.out.print("Coordinate nella write: ");
        for(int i = 0; i < coords.size(); i++){
            out.writeDouble(coords.get(i));
            //System.out.print(coords.get(i) + " ");
        }
        //System.out.println(" ");
        out.writeInt(occurrences);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        //System.out.println("Size dell read: " + size);
        //System.out.print("Coordinate nella read: ");
        coords = new ArrayList<Double>();
        for (int i = 0; i < size; i++){
            double e = in.readDouble();
            coords.add(e);
            //System.out.print(coords.get(i) + " ");
        }
        //System.out.println(" ");
        occurrences = in.readInt();
    }

    public void add(Point p){
        /*
        double[] c = this.getVector();
        double[] points = p.getVector();
        for (int i = 0; i < this.coords.size(); i++)
            c[i] += points[i];
        this.occurrences.set(this.occurrences.get() + p.getNumber());
        this.setVector(Arrays.copyOf(c, c.length));
        */
        System.out.println("[POINT_add]: This: " + this.toString());
        System.out.println("[POINT_add]: p: " + p.toString());
        System.out.print("[POINT_add]: sum: ");
        for (int i = 0; i < this.coords.size(); i++){
            this.coords.set(i, this.coords.get(i) + p.coords.get(i));
            System.out.print(this.coords.get(i) + " ");
        }
        System.out.println(" ");
        this.occurrences += p.occurrences;
    }

    public void avg(){
        /*
        double[] c = this.getVector();
        for (int i = 0; i < c.length; i++)
           c[i] = c[i] / this.occurrences.get();
        this.setVector(Arrays.copyOf(c, c.length));
        */
        for(int i = 0; i < this.coords.size(); i++)
            this.coords.set(i, this.coords.get(i) / this.occurrences);
    }

    public String getStringCoords(){
        String coord = "";
        for(int i = 0; i < coords.size(); i++)
            coord += coords.get(i) + " ";
        return coord;
    }

    public double getDistance(Point p1){
        //double[] p1Vector = p1.getVector();
        //double[] p2Vector = this.getVector();
        double sum = 0.0;
        //System.out.println("Dimensione vettore p1: " + p1.coords.size());
        for (int i = 0; i < p1.coords.size(); i++){
            //System.out.println("Iterazione numero della getDistance: " + i);
            //System.out.println("Valore coordinata p1: " + p1.coords.get(i));
            //System.out.println("Valore coordinata p2: " + this.coords.get(i));
            sum += Math.pow(p1.coords.get(i) - this.coords.get(i), 2);
        }
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
