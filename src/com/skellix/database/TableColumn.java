package com.skellix.database;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.MappedByteBuffer;

public class TableColumn {

	private int size = 0;
	private MappedByteBuffer buffer = null;
	
	public TableColumn(int size, MappedByteBuffer buffer) {
		this.size = size;
		this.buffer = buffer;
	}
	
	public byte[] getBytes() {
		byte[] out = new byte[size];
		buffer.rewind();
		buffer.get(out, 0, size);
		return out;
	}
	
	public void setBytes(byte[] value) {
		buffer.rewind();
		buffer.put(value);
	}
	
	public String getString() {
		return new String(getBytes());
	}
	
	public void setString(String value) {
		byte[] out = new byte[size];
		byte[] valueBytes = value.getBytes();
		for (int i = 0 ; i < value.length() ; i ++) {
			out[i] = valueBytes[i];
		}
		buffer.rewind();
		buffer.put(out, 0, size);
	}

	public int getInt() {
		buffer.rewind();
		return buffer.getInt();
	}

	public void setInt(int value) {
		buffer.rewind();
		buffer.putInt(value);
	}
	
	public char getChar() {
		buffer.rewind();
		return buffer.getChar();
	}
	
	public void setChar(char value) {
		buffer.rewind();
		buffer.putChar(value);
	}
	
	public short getShort() {
		buffer.rewind();
		return buffer.getShort();
	}
	
	public void setShort(short value) {
		buffer.rewind();
		buffer.putShort(value);
	}
	
	public float getFloat() {
		buffer.rewind();
		return buffer.getFloat();
	}
	
	public void setFloat(float value) {
		buffer.rewind();
		buffer.putFloat(value);
	}
	
	public double getDouble() {
		buffer.rewind();
		return buffer.getDouble();
	}
	
	public void setDouble(double value) {
		buffer.rewind();
		buffer.putDouble(value);
	}
	
	public boolean getBoolean() {
		buffer.rewind();
		return buffer.getInt() != 0;
	}
	
	public void setBoolean(boolean value) {
		buffer.rewind();
		buffer.putInt(value ? 1 : 0);
	}
	
	public Object getObject() {
		byte[] bytes = getBytes();
		ByteArrayInputStream arrayInputStream = new ByteArrayInputStream(bytes);
		try {
			ObjectInputStream inputStream = new ObjectInputStream(arrayInputStream);
			Object value = inputStream.readObject();
			inputStream.close();
			return value;
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public void setObject(Object value) {
		ByteArrayOutputStream arrayOutputStream = new ByteArrayOutputStream();
		try {
			ObjectOutputStream objectOutputStream = new ObjectOutputStream(arrayOutputStream);
			objectOutputStream.writeObject(value);
			objectOutputStream.flush();
			setBytes(arrayOutputStream.toByteArray());
			objectOutputStream.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

}
