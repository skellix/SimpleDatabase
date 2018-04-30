package com.skellix.database.table;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.MappedByteBuffer;

public class TableRow {

	private MappedByteBuffer buffer;
	private int offset;
	private int size;

	private TableRow(MappedByteBuffer buffer, int offset, int size) {
		
		this.buffer = buffer;
		this.offset = offset;
		this.size = size;
	}

	public static TableRow map(MappedByteBuffer buffer, int offset, int rowSize) {
		
		return new TableRow(buffer, offset, rowSize);
	}
	
	public byte[] getBytes(int columnOffset) {
		buffer.position(offset + columnOffset);
		byte[] out = new byte[size];
		buffer.get(out, 0, size);
		return out;
	}
	
	public void setBytes(int columnOffset, byte[] value) {
		buffer.position(offset + columnOffset);
		buffer.put(value);
	}
	
	public String getString(int columnOffset) {
		
		buffer.position(offset + columnOffset);
		
		for (int i = 0 ; i < size ; i ++) {
			
			byte b = buffer.get();
			
			if (b == 0) {
				
				return new String(getBytes(columnOffset), 0, i);
			}
		}
		
		return null;
	}
	
	public void setString(int columnOffset, String value) {
		setBytes(columnOffset, value.getBytes());
	}

	public int getInt(int columnOffset) {
		buffer.position(offset + columnOffset);
		return buffer.getInt();
	}

	public void setInt(int columnOffset, int value) {
		buffer.position(offset + columnOffset);
		buffer.putInt(value);
	}
	
	public long getLong(int columnOffset) {
		buffer.position(offset + columnOffset);
		return buffer.getLong();
	}

	public void setLong(int columnOffset, long value) {
		buffer.position(offset + columnOffset);
		buffer.putLong(value);
	}
	
	public char getChar(int columnOffset) {
		buffer.position(offset + columnOffset);
		return buffer.getChar();
	}
	
	public void setChar(int columnOffset, char value) {
		buffer.position(offset + columnOffset);
		buffer.putChar(value);
	}
	
	public short getShort(int columnOffset) {
		buffer.position(offset + columnOffset);
		return buffer.getShort();
	}
	
	public void setShort(int columnOffset, short value) {
		buffer.position(offset + columnOffset);
		buffer.putShort(value);
	}
	
	public float getFloat(int columnOffset) {
		buffer.position(offset + columnOffset);
		return buffer.getFloat();
	}
	
	public void setFloat(int columnOffset, float value) {
		buffer.position(offset + columnOffset);
		buffer.putFloat(value);
	}
	
	public double getDouble(int columnOffset) {
		buffer.position(offset + columnOffset);
		return buffer.getDouble();
	}
	
	public void setDouble(int columnOffset, double value) {
		buffer.position(offset + columnOffset);
		buffer.putDouble(value);
	}
	
	public boolean getBoolean(int columnOffset) {
		buffer.position(offset + columnOffset);
		return buffer.getInt() != 0;
	}
	
	public void setBoolean(int columnOffset, boolean value) {
		buffer.position(offset + columnOffset);
		buffer.putInt(value ? 1 : 0);
	}
	
	public Object getObject(int columnOffset) {
		byte[] bytes = getBytes(columnOffset);
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
	
	public void setObject(int columnOffset, Object value) {
		ByteArrayOutputStream arrayOutputStream = new ByteArrayOutputStream();
		try {
			ObjectOutputStream objectOutputStream = new ObjectOutputStream(arrayOutputStream);
			objectOutputStream.writeObject(value);
			objectOutputStream.flush();
			setBytes(columnOffset, arrayOutputStream.toByteArray());
			objectOutputStream.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
