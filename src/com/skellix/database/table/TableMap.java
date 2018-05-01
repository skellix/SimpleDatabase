package com.skellix.database.table;

import java.nio.MappedByteBuffer;
import java.util.AbstractMap.SimpleEntry;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;

public class TableMap {

	public static final int mapLengthValueSize = Integer.BYTES;
	public static final int typeSize = Byte.BYTES;
	public static final int pointerSize = Integer.BYTES;
	public static final int lengthSize = Integer.BYTES;
	public static final int defaultNumberOfPointers = 10;
	public static final int indexOfNextMapSize = Integer.BYTES;
	
	public static final byte ENTRY_TYPE_FREE = 0;
	public static final byte ENTRY_TYPE_ROWS = 1;
	
	public static Map<String, Map<Integer, TableMap>> openTableMaps = new LinkedHashMap<>();
	
	public int position;
	public int numberOfEntries = 0;
	public byte[] entryTypes = new byte[defaultNumberOfPointers];
	public int[] entryPointers = new int[defaultNumberOfPointers];
	public int[] entryLengths = new int[defaultNumberOfPointers];
	public int indexOfNextMap = 0;
	
	/*
	 * * Updated read method to use instance if it's already opened
	 * * Added debugPrint method to help with debugging
	 * * Added a method to get a block by position
	 * * added a method to add an entry to the map
	 */
	
	public int length() {
		
		int entryDataLength = entryDataLength();
		int result = mapLengthValueSize;
		result += (typeSize + pointerSize + lengthSize) * entryDataLength;
		result += indexOfNextMapSize;
		
		return result;
	}

	public int entryDataLength() {
		
		return Math.max(defaultNumberOfPointers, numberOfEntries);
	}

	public static TableMap read(ExperimentalTable table, int position) {
		
		Map<Integer, TableMap> openMaps = openTableMaps.get(table.uid());
		
		if (openMaps == null) {
			
			openTableMaps.put(table.uid(), openMaps = new LinkedHashMap<>());
		}
		
		TableMap alreadyOpenMap = openMaps.get(position);
		
		if (alreadyOpenMap != null) {
			
			return alreadyOpenMap;
		}
		
		MappedByteBuffer buffer = table.buffer;
		TableMap tableMap = new TableMap();
		buffer.position(position);
		
		tableMap.position = position;
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
		
		openMaps.put(position, tableMap);
		
		return tableMap;
	}
	
	public void writeAt(MappedByteBuffer buffer, int position) {
		
		this.position = position;
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

	public void removeData(int entryIndex, int rowOffset, int rowSize, ExperimentalTable table) {
		
		int beforeStartOffset = entryPointers[entryIndex];
		int beforeNewLength = rowOffset - beforeStartOffset;
		
		int afterStartOffset = rowOffset + rowSize;
		int afterNewLength = entryLengths[entryIndex] - beforeNewLength - rowSize;
		
		if (afterNewLength > 0) {
			
			addEntry(table, ENTRY_TYPE_FREE, rowOffset, rowSize);
			addEntry(table, ENTRY_TYPE_ROWS, afterStartOffset, afterNewLength);
			entryLengths[entryIndex] = beforeNewLength;
			
		} else {
			
			Entry<TableMap, Integer> nextBlock = getBlockAt(table, afterStartOffset);
			
			if (nextBlock == null) {
				
				addEntry(table, ENTRY_TYPE_FREE, rowOffset, rowSize);
				
			} else {
				
				TableMap nextBlockMap = nextBlock.getKey();
				int nextEntryIndex = nextBlock.getValue();
				
				if (nextBlockMap.entryTypes[nextEntryIndex] == ENTRY_TYPE_FREE) {
					
					nextBlockMap.entryPointers[nextEntryIndex] -= rowSize;
					nextBlockMap.entryLengths[nextEntryIndex] += rowSize;
					
				} else {

					addEntry(table, ENTRY_TYPE_FREE, rowOffset, rowSize);
				}
			}
			entryLengths[entryIndex] = beforeNewLength;
		}
		writeAt(table.buffer, position);
	}

	public static void addEntry(ExperimentalTable table, byte entryType, int entryPointer, int entryLength) {
		
		Entry<TableMap, Integer> emptyEntry = table.tableMapStream()
				.flatMap(tableMap -> {
					
					return Stream.iterate(0, i -> i + 1)
						.limit(tableMap.numberOfEntries)
						.filter(i -> tableMap.entryLengths[i] == 0)
						.map(i -> new SimpleEntry<>(tableMap, i));
				})
				.findFirst().orElse(null);
		
		if (emptyEntry != null) {
			
			TableMap tableMap = emptyEntry.getKey();
			Integer emptyIndex = emptyEntry.getValue();
			
			tableMap.entryTypes[emptyIndex] = entryType;
			tableMap.entryPointers[emptyIndex] = entryPointer;
			tableMap.entryLengths[emptyIndex] = entryLength;
			return;
		}
		
		TableMap tableMap = table.tableMapStream()
			.filter(tableMap2 -> tableMap2.numberOfEntries < tableMap2.entryDataLength())
			.findFirst().orElse(null);
		
		if (tableMap == null) {
			
			TableMap lastTable = getLastTableMap(table);
			
			if (lastTable == null) {
				
				System.err.println("Corrupted table: Unable to find last table map");
				System.exit(-1);
			}
			
			TableMap newMap = new TableMap();
			int position = table.buffer.limit();
			table.resize(position + newMap.length());
			int index = newMap.numberOfEntries ++;
			newMap.entryTypes[index] = entryType;
			newMap.entryPointers[index] = entryPointer;
			newMap.entryLengths[index] = entryLength;
			newMap.writeAt(table.buffer, position);
			return;
		}
		
		int index = tableMap.numberOfEntries ++;
		tableMap.entryTypes[index] = entryType;
		tableMap.entryPointers[index] = entryPointer;
		tableMap.entryLengths[index] = entryLength;
		tableMap.writeAt(table.buffer, tableMap.position);
	}

	private static TableMap getLastTableMap(ExperimentalTable table) {
		
		int count = (int) table.tableMapStream().count();
		TableMap lastTable = table.tableMapStream().skip(count - 1).findFirst().orElse(null);
		return lastTable;
	}

	public static Entry<TableMap, Integer> getBlockBefore(ExperimentalTable table, int offset) {
		
		Entry<TableMap, Integer> nextBlock = table.tableMapStream()
			.flatMap(tableMap -> {
			
				return Stream.iterate(0, i -> i + 1)
					.limit(tableMap.numberOfEntries)
					.filter(i -> tableMap.entryPointers[i] + tableMap.entryLengths[i] == offset)
					.map(i -> new SimpleEntry<TableMap, Integer>(tableMap, i));
				
			}).findFirst().orElse(null);
		
		return nextBlock;
	}
	
	public static Entry<TableMap, Integer> getBlockAt(ExperimentalTable table, int offset) {
		
		Entry<TableMap, Integer> nextBlock = table.tableMapStream()
			.flatMap(tableMap -> {
			
				return Stream.iterate(0, i -> i + 1)
					.limit(tableMap.numberOfEntries)
					.filter(i -> tableMap.entryPointers[i] == offset)
					.map(i -> new SimpleEntry<TableMap, Integer>(tableMap, i));
				
			}).findFirst().orElse(null);
		
		return nextBlock;
	}

	public void debugPrint() {
		
		System.out.print(toString());
	}
	
	@Override
	public String toString() {
		
		StringBuilder sb = new StringBuilder();
		sb.append("TableMap------------------\n");
		sb.append(String.format("  position:       %d\n", position));
		sb.append(String.format("  entries:        %d\n", numberOfEntries));
		
		for (int i = 0 ; i < numberOfEntries ; i ++) {
			
			byte entryType = entryTypes[i];
			sb.append("    ");
			sb.append(entryType == ENTRY_TYPE_FREE ? "free" : "rows");
			sb.append(String.format(" %4db @ %d"
					, entryLengths[i]
					, entryPointers[i]));
			
			sb.append('\n');
		}
		
		sb.append(String.format("  indexOfNextMap: %d\n", indexOfNextMap));
		return sb.toString();
	}
}
