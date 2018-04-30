package com.skellix.database.table;

import java.nio.MappedByteBuffer;

public class TableMap {

	public static final int mapLengthValueSize = Integer.BYTES;
	public static final int typeSize = Byte.BYTES;
	public static final int pointerSize = Integer.BYTES;
	public static final int lengthSize = Integer.BYTES;
	public static final int defaultNumberOfPointers = 10;
	public static final int indexOfNextMapSize = Integer.BYTES;
	
	public static final byte ENTRY_TYPE_FREE = 0;
	public static final byte ENTRY_TYPE_ROWS = 1;
	
	public int numberOfEntries = 0;
	public byte[] entryTypes = new byte[defaultNumberOfPointers];
	public int[] entryPointers = new int[defaultNumberOfPointers];
	public int[] entryLengths = new int[defaultNumberOfPointers];
	public int indexOfNextMap = 0;
	
	public int length() {
		
		int entryDataLength = entryDataLength();
		int result = mapLengthValueSize;
		result += (typeSize + pointerSize + lengthSize) * entryDataLength;
		result += indexOfNextMapSize;
		
		return result;
	}

	private int entryDataLength() {
		
		return Math.max(defaultNumberOfPointers, numberOfEntries);
	}

	public void writeAt(MappedByteBuffer buffer, int position) {
		
		buffer.position(position);
		buffer.putInt(numberOfEntries);
		
		int entryDataLength = entryDataLength();
		
		for (int i = 0 ; i < entryDataLength ; i ++) {
			
			buffer.put(entryTypes[i]);
			buffer.putInt(entryPointers[i]);
			buffer.putInt(entryLengths[i]);
		}
		
		buffer.putInt(indexOfNextMap);
	}

	public static TableMap read(MappedByteBuffer buffer, int position) {
		
		TableMap tableMap = new TableMap();
		buffer.position(position);
		
		tableMap.numberOfEntries = buffer.getInt();
		int entryDataLength = tableMap.entryDataLength();
		
		tableMap.entryPointers = new int[entryDataLength];
		tableMap.entryLengths = new int[entryDataLength];
		
		for (int i = 0 ; i < entryDataLength ; i ++) {
			
			tableMap.entryTypes[i] = buffer.get();
			tableMap.entryPointers[i] = buffer.getInt();
			tableMap.entryLengths[i] = buffer.getInt();
		}
		
		tableMap.indexOfNextMap = buffer.getInt();
		
		return tableMap;
	}
}
