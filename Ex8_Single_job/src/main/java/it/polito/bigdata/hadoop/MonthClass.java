package it.polito.bigdata.hadoop;

import java.io.IOException;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class MonthClass implements Writable {
    private String monthID;
    private double income;

    public MonthClass(String mid, double inc) {
        this.monthID = mid;
        this.income = inc;
    }

    public MonthClass() {
        this.monthID = new String();
        this.income = 0.0;
    }

    public String getMonthID() {
        return this.monthID;
    }

    public double getIncome() {
        return this.income;
    }

    public void setMonthID(String mid) {
        this.monthID = mid;
    }

    public void setIncome(double inc) {
        this.income = inc;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.monthID = in.readUTF();
        this.income = in.readDouble();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.monthID);
        out.writeDouble(this.income);
    }
}