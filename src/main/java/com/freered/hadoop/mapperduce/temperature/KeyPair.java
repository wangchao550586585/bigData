package com.freered.hadoop.mapperduce.temperature;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * 自定义封装类
 * 
 * @author Administrator
 *
 */
public class KeyPair implements WritableComparable<KeyPair> {

	private int year;
	private int temperature;

	public KeyPair(int year, int temperature) {
		this.year = year;
		this.temperature = temperature;
	}

	public KeyPair() {
	}

	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}

	public int getTemperature() {
		return temperature;
	}

	public void setTemperature(int temperature) {
		this.temperature = temperature;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(year);
		out.writeInt(temperature);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.year = in.readInt();
		this.temperature = in.readInt();
	}

	@Override
	public int compareTo(KeyPair o) {
		int compare = Integer.compare(year, o.getYear());
		if (compare != 0)
			return compare;
		return Integer.compare(temperature, o.getTemperature());
	}

	public String toString() {
		return year + "\t" + temperature;
	}

	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + temperature;
		result = prime * result + year;
		return result;
	}

}
