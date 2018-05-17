package com.skellix.database.table;

import java.nio.MappedByteBuffer;
import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;

import com.skellix.database.row.TableRow;

public class TableMap {

	public static final int mapLengthValueSize = Integer.BYTES;
	public static final int typeSize = Byte.BYTES;
	public static final int pointerSize = Integer.BYTES;
	public static final int lengthSize = Integer.BYTES;
	public static final int defaultNumberOfPointers = 10;
//	public static final int indexOfNextMapSize = Integer.BYTES;
	
	public static final byte ENTRY_TYPE_FREE = 0;
	public static final byte ENTRY_TYPE_ROWS = 1;
	
	public static Map<String, Map<Integer, TableMap>> openTableMaps = new LinkedHashMap<>();
	
	public int position;
	public int numberOfEntries = 0;
	public byte[] entryTypes = new byte[defaultNumberOfPointers];
	public int[] entryPointers = new int[defaultNumberOfPointers];
	public int[] entryLengths = new int[defaultNumberOfPointers];
//	public int indexOfNextMap = 0;
	
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
		//result += indexOfNextMapSize;
		
		return result;
	}

	public int entryDataLength() {
		
		return Math.max(defaultNumberOfPointers, numberOfEntries);
	}

	public static TableMap read(Table table, int position) {
		
		Map<Integer, TableMap> openMaps = openTableMaps.get(table.uid());
		
		if (openMaps == null) {
			
			openTableMaps.put(table.uid(), openMaps = new LinkedHashMap<>());
		}
		
		TableMap alreadyOpenMap = openMaps.get(position);
		
		if (alreadyOpenMap != null) {
			
			return alreadyOpenMap;
		}
		
		MappedByteBuffer buffer = table.tableMapBuffer;
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
		
//		tableMap.indexOfNextMap = buffer.getInt();
		
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
		
//		buffer.putInt(indexOfNextMap);
	}
	
	public static void removeMapsForTable(Table table) {
		
		openTableMaps.remove(table.uid());
	}
//
//	public void removeData(int entryIndex, int rowOffset, int rowSize, ExperimentalTable table) {
//		
//		int beforeStartOffset = entryPointers[entryIndex];
//		int beforeNewLength = rowOffset - beforeStartOffset;
//		
//		int afterStartOffset = rowOffset + rowSize;
//		int afterNewLength = entryLengths[entryIndex] - beforeNewLength - rowSize;
//		
//		if (afterNewLength > 0) {
//			
//			addEntry(table, ENTRY_TYPE_FREE, rowOffset, rowSize);
//			addEntry(table, ENTRY_TYPE_ROWS, afterStartOffset, afterNewLength);
//			entryLengths[entryIndex] = beforeNewLength;
//			
//		} else {
//			
//			Entry<TableMap, Integer> nextBlock = getBlockAt(table, afterStartOffset);
//			
//			if (nextBlock == null) {
//				
//				addEntry(table, ENTRY_TYPE_FREE, rowOffset, rowSize);
//				
//			} else {
//				
//				TableMap nextBlockMap = nextBlock.getKey();
//				int nextEntryIndex = nextBlock.getValue();
//				
//				if (nextBlockMap.entryTypes[nextEntryIndex] == ENTRY_TYPE_FREE) {
//					
//					nextBlockMap.entryPointers[nextEntryIndex] -= rowSize;
//					nextBlockMap.entryLengths[nextEntryIndex] += rowSize;
//					
//				} else {
//
//					addEntry(table, ENTRY_TYPE_FREE, rowOffset, rowSize);
//				}
//			}
//			entryLengths[entryIndex] = beforeNewLength;
//		}
//		writeAt(table.tableMapBuffer, position);
//	}
//	
	public TableRow addRow(Table table, int rowSize) {
		
		if (numberOfEntries == 0) {
			
			int offset = table.buffer.limit();
			table.resize(offset + rowSize);
			addEntry(TableMap.ENTRY_TYPE_ROWS, offset, rowSize);
			return TableRow.map(table, offset, rowSize);
			
		} else {
			
			int indexOfFirstFreeEntry = indexOfFirstFreeEntry();
			
			if (indexOfFirstFreeEntry == -1) {
				
				int indexOfLastRowEntry = numberOfEntries - 1;
				int offset = table.buffer.limit();
				table.resize(offset + rowSize);
				entryLengths[indexOfLastRowEntry] += rowSize;
				return TableRow.map(table, offset, rowSize);
			}

			if (indexOfFirstFreeEntry > 0) {
				
				int index = indexOfFirstFreeEntry;
				
				do {
					int indexOfLastEntry = indexOfEntryBefore(index);
					
					if (indexOfLastEntry == -1) {
						
						break;
					}
					
					int entryType = entryTypes[indexOfLastEntry];
					
					if (entryType == ENTRY_TYPE_ROWS) {
						
						int indexOfLastRowEntry = indexOfLastEntry;
						int offset = entryPointers[indexOfLastRowEntry] + entryLengths[indexOfLastRowEntry];
						entryLengths[indexOfLastRowEntry] += rowSize;
						entryPointers[index] += rowSize;
						entryLengths[index] -= rowSize;
						
						if (entryLengths[index] == 0) {
							
							removeEntry(index);
							combineEntryWithNextIfNeeded(indexOfLastRowEntry);
						}
						
						return TableRow.map(table, offset, rowSize);
					}
					
					index = indexOfLastEntry;
				
				} while (true);
			}
			
			int offset = entryPointers[indexOfFirstFreeEntry];
			entryPointers[indexOfFirstFreeEntry] += rowSize;
			entryLengths[indexOfFirstFreeEntry] -= rowSize;
			
			if (entryLengths[indexOfFirstFreeEntry] == 0) {
				
				removeEntry(indexOfFirstFreeEntry);
			}
			
			addEntry(TableMap.ENTRY_TYPE_ROWS, offset, rowSize);
			return TableRow.map(table, offset, rowSize);
		}
	}
	
	private void combineEntryWithNextIfNeeded(int index) {
		
		int nextIndex = indexOfEntryAfter(index);
		
		if (nextIndex != -1) {
			
			byte entryType = entryTypes[index];
			byte nextEntryType = entryTypes[nextIndex];
			
			if (entryType == nextEntryType) {
				
				entryLengths[index] += entryLengths[nextIndex];
				removeEntry(nextIndex);
			}
		}
	}

	private void removeEntry(int index) {
		
		int iEnd = numberOfEntries - 1;
		
		for (int i = index ; i < iEnd ; i ++) {
			
			entryLengths[i] = entryLengths[i + 1];
			entryPointers[i] = entryPointers[i + 1];
			entryTypes[i] = entryTypes[i + 1];
		}
		
		entryLengths[iEnd] = 0;
		entryPointers[iEnd] = 0;
		entryTypes[iEnd] = 0;
		
		numberOfEntries --;
	}

	private int indexOfEntryBefore(int index) {
		
		int entryPointer = entryPointers[index];
		
		for (int i = 0 ; i < numberOfEntries ; i++) {
			
			int entryPointerI = entryPointers[i];
			int entryLengthI = entryLengths[i];
			
			if (entryPointerI + entryLengthI == entryPointer) {
				
				return i;
			}
		}
		
		return -1;
	}
	
	private int indexOfEntryAfter(int index) {
		
		int targetPointer = entryPointers[index] + entryLengths[index];
		
		for (int i = 0 ; i < numberOfEntries ; i++) {
			
			int entryPointer = entryPointers[i];
			
			if (entryPointer == targetPointer) {
				
				return i;
			}
		}
		
		return -1;
	}

	private int indexOfFirstFreeEntry() {
		
		for (int i = 0 ; i < numberOfEntries ; i++) {
			
			byte type = entryTypes[i];
			
			if (type == ENTRY_TYPE_FREE) {
				
				return i;
			}
		}
		
		return -1;
	}

	private int addEntry(byte entryType, int entryPointer, int entryLength) {
		
		int index = addEntry();
		entryTypes[index] = entryType;
		entryPointers[index] = entryPointer;
		entryLengths[index] = entryLength;
		return index;
	}

	private int addEntry() {

		numberOfEntries ++;
		
		if (numberOfEntries >= entryPointers.length) {
			
			int newSize = numberOfEntries - numberOfEntries % defaultNumberOfPointers + defaultNumberOfPointers;
			entryLengths = Arrays.copyOf(entryLengths, newSize);
			entryPointers = Arrays.copyOf(entryPointers, newSize);
			entryTypes = Arrays.copyOf(entryTypes, newSize);
		}
		
		return numberOfEntries - 1;
	}

//	public int findFreeSpace(ExperimentalTable table, int rowSize) {
//		
//		for (int i = 0 ; i < entryTypes.length ; i++) {
//			
//			byte entryType = entryTypes[i];
//			
//			if (entryType == ENTRY_TYPE_FREE) {
//				
//				int entryPointer = entryPointers[i];
//				int entryLength = entryLengths[i];
//			}
//		}
//		
//		return 0;
//	}
	
	public void free(int offset, int rowSize) throws Exception {
		
		int indexOfEntry = indexOfEntryContaining(offset);
		
		if (indexOfEntry == -1) {
			
			throw new Exception("The memory at offset = " + offset + " is not in the table map");
		}
		
		int entryPointer = entryPointers[indexOfEntry];
		int entryLength = entryLengths[indexOfEntry];
		
		if (offset == entryPointer) {
			
			int indexOfEntryBefore = indexOfEntryBefore(indexOfEntry);
			
			if (indexOfEntryBefore != -1) {
				
				byte typeBefore = entryTypes[indexOfEntryBefore];
				
				if (typeBefore == ENTRY_TYPE_FREE) {
					
					entryLengths[indexOfEntryBefore] += rowSize;
					entryPointers[indexOfEntry] += rowSize;
					entryLengths[indexOfEntry] -= rowSize;
					
					if (entryLengths[indexOfEntry] == 0) {
						
						removeEntry(indexOfEntry);
						combineEntryWithNextIfNeeded(indexOfEntryBefore);
					}
					
					return;
				}
			}
			
			entryPointers[indexOfEntry] += rowSize;
			entryLengths[indexOfEntry] -= rowSize;
			int newEntryIndex = addEntry(TableMap.ENTRY_TYPE_FREE, offset, rowSize);
			
			if (entryLengths[indexOfEntry] == 0) {
				
				removeEntry(indexOfEntry);
			}
			
			combineEntryWithNextIfNeeded(newEntryIndex);
			
		} else if (offset == entryPointer + entryLength - rowSize) {
			
			entryLengths[indexOfEntry] -= rowSize;
			int newEntryIndex = addEntry(TableMap.ENTRY_TYPE_FREE, offset, rowSize);
			combineEntryWithNextIfNeeded(newEntryIndex);
			
		} else {
			
			int pointerBefore = entryPointer;
			int lengthBefore = offset - entryPointer;
			int freePointer = offset;
			int freeLength = rowSize;
			int pointerAfter = offset + rowSize;
			int lengthAfter = (entryLength - lengthBefore) - freeLength;
			entryLengths[indexOfEntry] = lengthBefore;
			int freeEntryIndex = addEntry(TableMap.ENTRY_TYPE_FREE, freePointer, freeLength);
			
			if (lengthAfter > 0) {
				
				int entryIndexAfter = addEntry(TableMap.ENTRY_TYPE_ROWS, pointerAfter, lengthAfter);
				combineEntryWithNextIfNeeded(entryIndexAfter);
				
			} else {
				
				combineEntryWithNextIfNeeded(freeEntryIndex);
			}
		}
	}

	private int indexOfEntryContaining(int offset) {
		
		for (int i = 0 ; i < numberOfEntries ; i++) {
			
			int entryPointer = entryPointers[i];
			int entryLength = entryLengths[i];
			
			if (offset >= entryPointer && offset < entryPointer + entryLength) {
				
				return i;
			}
		}
		
		return -1;
	}
//
//	public static void addEntry(ExperimentalTable table, byte entryType, int entryPointer, int entryLength) {
//		
//		Entry<TableMap, Integer> emptyEntry = table.tableMapStream()
//				.flatMap(tableMap -> {
//					
//					return Stream.iterate(0, i -> i + 1)
//						.limit(tableMap.numberOfEntries)
//						.filter(i -> tableMap.entryLengths[i] == 0)
//						.map(i -> new SimpleEntry<>(tableMap, i));
//				})
//				.findFirst().orElse(null);
//		
//		if (emptyEntry != null) {
//			
//			TableMap tableMap = emptyEntry.getKey();
//			Integer emptyIndex = emptyEntry.getValue();
//			
//			tableMap.entryTypes[emptyIndex] = entryType;
//			tableMap.entryPointers[emptyIndex] = entryPointer;
//			tableMap.entryLengths[emptyIndex] = entryLength;
//			return;
//		}
//		
//		TableMap tableMap = table.tableMapStream()
//			.filter(tableMap2 -> tableMap2.numberOfEntries < tableMap2.entryDataLength())
//			.findFirst().orElse(null);
//		
//		if (tableMap == null) {
//			
//			TableMap lastTable = getLastTableMap(table);
//			
//			if (lastTable == null) {
//				
//				System.err.println("Corrupted table: Unable to find last table map");
//				System.exit(-1);
//			}
//			
//			TableMap newMap = new TableMap();
//			int position = table.tableMapBuffer.limit();
//			table.resize(position + newMap.length());
//			int index = newMap.numberOfEntries ++;
//			newMap.entryTypes[index] = entryType;
//			newMap.entryPointers[index] = entryPointer;
//			newMap.entryLengths[index] = entryLength;
//			newMap.writeAt(table.tableMapBuffer, position);
//			return;
//		}
//		
//		int index = tableMap.numberOfEntries ++;
//		tableMap.entryTypes[index] = entryType;
//		tableMap.entryPointers[index] = entryPointer;
//		tableMap.entryLengths[index] = entryLength;
//		tableMap.writeAt(table.tableMapBuffer, tableMap.position);
//	}
//
//	private static TableMap getLastTableMap(ExperimentalTable table) {
//		
//		int count = (int) table.tableMapStream().count();
//		TableMap lastTable = table.tableMapStream().skip(count - 1).findFirst().orElse(null);
//		return lastTable;
//	}
//
//	public static Entry<TableMap, Integer> getBlockBefore(ExperimentalTable table, int offset) {
//		
//		Entry<TableMap, Integer> nextBlock = table.tableMapStream()
//			.flatMap(tableMap -> {
//			
//				return Stream.iterate(0, i -> i + 1)
//					.limit(tableMap.numberOfEntries)
//					.filter(i -> tableMap.entryPointers[i] + tableMap.entryLengths[i] == offset)
//					.map(i -> new SimpleEntry<TableMap, Integer>(tableMap, i));
//				
//			}).findFirst().orElse(null);
//		
//		return nextBlock;
//	}
//	
//	public static Entry<TableMap, Integer> getBlockAt(ExperimentalTable table, int offset) {
//		
//		Entry<TableMap, Integer> nextBlock = table.tableMapStream()
//			.flatMap(tableMap -> {
//			
//				return Stream.iterate(0, i -> i + 1)
//					.limit(tableMap.numberOfEntries)
//					.filter(i -> tableMap.entryPointers[i] == offset)
//					.map(i -> new SimpleEntry<TableMap, Integer>(tableMap, i));
//				
//			}).findFirst().orElse(null);
//		
//		return nextBlock;
//	}

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
		
//		sb.append(String.format("  indexOfNextMap: %d\n", indexOfNextMap));
		return sb.toString();
	}

}
