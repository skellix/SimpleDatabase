package com.skellix.database.table;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

public class ExperimentalTable {
	
	private Path tableFile;
	public MappedByteBuffer buffer;
	public Map<String, Integer> rowFormat;
	public Map<String, Integer> columnOffset;
	public int rowSize = 0;
	
	public ExperimentalTable(Path directory, Map<String, Integer> rowFormat) {
		
		initDir(directory);
		
		tableFile = directory.resolve("table.bin");
		
		try (RandomAccessFile randomAccessFile = new RandomAccessFile(tableFile.toFile(), "rw")) {
			
			buffer = randomAccessFile.getChannel().map(MapMode.READ_WRITE, 0, Files.size(tableFile));
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		if (buffer.limit() == 0) {
			
			initTable();
		}
		
		this.rowFormat = rowFormat;
		columnOffset = new LinkedHashMap<>();
		
		for (String key : rowFormat.keySet()) {
			
			columnOffset.put(key, rowSize);
			rowSize += rowFormat.get(key);
		}
	}
	
	public void deleteTable() throws IOException {
		
		System.out.println("deleting: " + tableFile.toAbsolutePath().toString());
		Files.delete(tableFile);
		System.out.println("deleted: " + tableFile.toAbsolutePath().toString());
	}
	
	public TableRow addRow() {
		
		int tableMapPointer = 0;
		
		while (tableMapPointer < buffer.limit()) {
			
			TableMap tableMap = TableMap.read(this, tableMapPointer);
			
			if (tableMap.numberOfEntries == 0) {
				
				int offset = buffer.limit();
				resize(offset + rowSize);
				TableMap.addEntry(this, TableMap.ENTRY_TYPE_ROWS, offset, rowSize);
				return TableRow.map(buffer, offset, rowSize);
			}
			
			for (int i = 0 ; i < tableMap.numberOfEntries ; i ++) {
				
				int entryType = tableMap.entryTypes[i];
				int entryPointer = tableMap.entryPointers[i];
				
				if (entryType == TableMap.ENTRY_TYPE_FREE) {
					if (tableMap.entryLengths[i] >= rowSize) {
						
						Entry<TableMap, Integer> tableMapBefore = TableMap.getBlockBefore(this, entryPointer);
						
						if (tableMapBefore != null) {
							
							TableMap mapBefore = tableMapBefore.getKey();
							Integer entryBeforeIndex = tableMapBefore.getValue();
							byte entryBeforeType = mapBefore.entryTypes[entryBeforeIndex];
							
							if (entryBeforeType == TableMap.ENTRY_TYPE_ROWS) {
								
								int offset = mapBefore.entryPointers[entryBeforeIndex] + mapBefore.entryLengths[entryBeforeIndex];
								mapBefore.entryLengths[entryBeforeIndex] += rowSize;
								tableMap.entryPointers[i] += rowSize;
								tableMap.entryLengths[i] -= rowSize;
								mapBefore.writeAt(buffer, mapBefore.position);
								tableMap.writeAt(buffer, tableMap.position);
								return TableRow.map(buffer, offset, rowSize);
							}
						}
						
						Entry<TableMap, Integer> tableMapAfter = TableMap.getBlockAt(this, entryPointer + tableMap.entryLengths[i]);
						
						if (tableMapAfter != null) {
							
							TableMap mapAfter = tableMapAfter.getKey();
							Integer entryAfterIndex = tableMapAfter.getValue();
							byte entryAfterType = mapAfter.entryTypes[entryAfterIndex];
							
							if (entryAfterType == TableMap.ENTRY_TYPE_ROWS) {
								
								int offset = mapAfter.entryPointers[entryAfterIndex];
								mapAfter.entryPointers[entryAfterIndex] += rowSize;
								mapAfter.entryLengths[entryAfterIndex] -= rowSize;
								tableMap.entryLengths[i] -= rowSize;
								mapAfter.writeAt(buffer, mapAfter.position);
								tableMap.writeAt(buffer, tableMap.position);
								return TableRow.map(buffer, offset, rowSize);
							}
						}
					}
				} else if (entryType == TableMap.ENTRY_TYPE_ROWS) {
					
					if (entryPointer + tableMap.entryLengths[i] == buffer.limit()) {
						
						int offset = buffer.limit();
						resize(offset + rowSize);
						tableMap.entryLengths[i] += rowSize;
						tableMap.writeAt(buffer, tableMap.position);
						return TableRow.map(buffer, offset, rowSize);
					}
					
					Entry<TableMap, Integer> tableMapBefore = TableMap.getBlockBefore(this, entryPointer);
					
					if (tableMapBefore != null) {
						
						TableMap mapBefore = tableMapBefore.getKey();
						Integer entryBeforeIndex = tableMapBefore.getValue();
						byte entryBeforeType = mapBefore.entryTypes[entryBeforeIndex];
						
						if (entryBeforeType == TableMap.ENTRY_TYPE_FREE) {
							if (mapBefore.entryLengths[entryBeforeIndex] >= rowSize) {
								
								int offset = entryPointer - rowSize;
								mapBefore.entryLengths[entryBeforeIndex] -= rowSize;
								tableMap.entryPointers[i] -= rowSize;
								tableMap.entryLengths[i] += rowSize;
								mapBefore.writeAt(buffer, mapBefore.position);
								tableMap.writeAt(buffer, tableMap.position);
								return TableRow.map(buffer, offset, rowSize);
							}
						}
					}
					
					Entry<TableMap, Integer> tableMapAfter = TableMap.getBlockAt(this, entryPointer + tableMap.entryLengths[i]);
					
					if (tableMapAfter != null) {
						
						TableMap mapAfter = tableMapAfter.getKey();
						Integer entryAfterIndex = tableMapAfter.getValue();
						byte entryAfterType = mapAfter.entryTypes[entryAfterIndex];
						
						if (entryAfterType == TableMap.ENTRY_TYPE_FREE) {
							if (mapAfter.entryLengths[entryAfterIndex] >= rowSize) {
								
								int offset = mapAfter.entryPointers[entryAfterIndex];
								mapAfter.entryPointers[entryAfterIndex] += rowSize;
								mapAfter.entryLengths[entryAfterIndex] -= rowSize;
								tableMap.entryLengths[i] += rowSize;
								mapAfter.writeAt(buffer, mapAfter.position);
								tableMap.writeAt(buffer, tableMap.position);
								return TableRow.map(buffer, offset, rowSize);
							}
						}
					}
				}
			}
			
			if (tableMap.indexOfNextMap == 0) {
				
				TableMap nextMap = new TableMap();
				int offset = buffer.limit();
				resize(offset + nextMap.length());
				nextMap.writeAt(buffer, offset);
				offset = buffer.limit();
				resize(offset + rowSize);
				tableMap.indexOfNextMap = nextMap.position;
				tableMap.writeAt(buffer, tableMap.position);
				TableMap.addEntry(this, TableMap.ENTRY_TYPE_ROWS, offset, rowSize);
				return TableRow.map(buffer, offset, rowSize);
			}
		}
		
		return null;
	}
	
	public String uid() {
		
		return tableFile.toAbsolutePath().toString();
	}
	
	private int numberOfTableMaps() {
		
		int count = 0;
		int tableMapPointer = 0;
		
		while (tableMapPointer < buffer.limit()) {
			
			TableMap tableMap = TableMap.read(this, tableMapPointer);
			
			count ++;
			
			if (tableMap.indexOfNextMap == 0) {
				
				break;
			}
			
			tableMapPointer = tableMap.indexOfNextMap;
		}
		
		return count;
	}
	
	public Stream<TableMap> tableMapStream() {
		
		Stream<Integer> stream = Stream.iterate(0, i -> i + 1);
		AtomicReference<Integer> tableMapPointerRef = new AtomicReference<Integer>(0);
		return stream.limit(numberOfTableMaps()).map(i -> {
			
			TableMap tableMap = TableMap.read(this, tableMapPointerRef.get());
			tableMapPointerRef.set(tableMap.indexOfNextMap);
			return tableMap;
		});
	}
	
	public void deleteRow(TableRow rowToDelete) {
		
		int rowOffset = rowToDelete.offset;
		
		int tableMapPointer = 0;
		
		while (tableMapPointer < buffer.limit()) {
			
			TableMap tableMap = TableMap.read(this, tableMapPointer);
			
			for (int i = 0 ; i < tableMap.numberOfEntries ; i ++) {
				
				int entryType = tableMap.entryTypes[i];
				int entryPointer = tableMap.entryPointers[i];
				int entryLength = tableMap.entryLengths[i];
				
				if (entryType == TableMap.ENTRY_TYPE_ROWS) {
					if (entryPointer + entryLength <= buffer.limit()) {
						
						if (rowOffset > entryPointer && rowOffset < entryPointer + entryLength) {
							
							tableMap.removeData(i, rowOffset, rowSize, this);
							return;
						}
					}
				}
			}
			
			if (tableMap.indexOfNextMap == 0) {
				
				break;
			}
			
			tableMapPointer = tableMap.indexOfNextMap;
		}
	}
	
//	private TableRow getRowNumber(int index) {
//		
//		int count = 0;
//		int tableMapPointer = 0;
//		
//		while (tableMapPointer < buffer.limit()) {
//			
//			TableMap tableMap = TableMap.read(buffer, tableMapPointer);
//			
//			for (int i = 0 ; i < tableMap.numberOfEntries ; i ++) {
//				
//				int entryType = tableMap.entryTypes[i];
//				int entryPointer = tableMap.entryPointers[i];
//				int entryLength = tableMap.entryLengths[i];
//				
//				if (entryType == TableMap.ENTRY_TYPE_ROWS) {
//					if (entryPointer + entryLength <= buffer.limit()) {
//						
//						int numEntries = entryLength / rowSize;
//						
//						if (index > count && index < count + numEntries) {
//							
//							int more = index - count;
//							int offset = entryPointer + more * rowSize;
//							return TableRow.map(buffer, offset, rowSize);
//						}
//						
//						count += numEntries;
//					}
//				}
//			}
//			
//			if (tableMap.indexOfNextMap == 0) {
//				
//				break;
//			}
//			
//			tableMapPointer = tableMap.indexOfNextMap;
//		}
//		
//		return null;
//	}
	
	public Stream<TableRow> stream() {
		
		return tableMapStream().flatMap(tableMap -> {
			
			Stream<Integer> entryIndexStream = Stream.iterate(0, i -> i + 1);
			
			return entryIndexStream.limit(tableMap.numberOfEntries)
					.filter(i -> tableMap.entryTypes[i] == TableMap.ENTRY_TYPE_ROWS)
					.flatMap(i -> {
						
						Stream<Integer> entryStream = Stream.iterate(tableMap.entryPointers[i], j -> j + rowSize);
						
						return entryStream.limit(tableMap.entryLengths[i] / rowSize)
								.map(j -> {
							
									return TableRow.map(buffer, j, rowSize);
								});
					});
		});
	}

	public int rowCount() {
		
		int count = 0;
		
		int tableMapPointer = 0;
		
		while (tableMapPointer < buffer.limit()) {
			
			TableMap tableMap = TableMap.read(this, tableMapPointer);
			
			for (int i = 0 ; i < tableMap.numberOfEntries ; i ++) {
				
				int entryType = tableMap.entryTypes[i];
				int entryPointer = tableMap.entryPointers[i];
				int entryLength = tableMap.entryLengths[i];
				
				if (entryType == TableMap.ENTRY_TYPE_ROWS) {
					if (entryPointer + entryLength <= buffer.limit()) {
						
						count += entryLength / rowSize;
					}
				}
			}
			
			if (tableMap.indexOfNextMap == 0) {
				
				break;
			}
			
			tableMapPointer = tableMap.indexOfNextMap;
		}
		
		return count;
	}
	
	public void initTable() {
		
		TableMap tableMap = new TableMap();
		resize(tableMap.length());
		tableMap.writeAt(buffer, 0);
	}

	public void resize(int size) {
		
		try (RandomAccessFile randomAccessFile = new RandomAccessFile(tableFile.toFile(), "rw")) {
			
			long oldSize = Files.size(tableFile);
			int diff = (int) (size - oldSize);
			randomAccessFile.setLength(size);
			buffer = randomAccessFile.getChannel().map(MapMode.READ_WRITE, 0, Files.size(tableFile));
			buffer.position((int) oldSize);
			buffer.put(new byte[diff]);
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
//	private MappedByteBuffer map(int offset, int length) {
//		
//		try (RandomAccessFile randomAccessFile = new RandomAccessFile(tableFile.toFile(), "rw")) {
//			
//			return randomAccessFile.getChannel().map(MapMode.READ_WRITE, offset, length);
//			
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//		
//		return null;
//	}

	private void initDir(Path directory) {
		
		if (directory == null) {
			System.err.printf("The init method for %s must initialize the directory field\n", this.getClass().getCanonicalName());
			System.exit(-1);
		}
		
		if (!Files.exists(directory)) {
			try {
				Files.createDirectories(directory);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public String[] getColumnLabels() {
		
		ArrayList<String> result = new ArrayList<>();
		
		for (String key : rowFormat.keySet()) {
			
			result.add(key);
		}
		
		return result.toArray(new String[0]);
	}

	public void debugPrint() {
		
		tableMapStream().forEach(tableMap -> {
			
			tableMap.debugPrint();
			
			Stream.iterate(0, i -> i + 1)
				.limit(tableMap.numberOfEntries)
				.forEach(i -> {
					
					if (tableMap.entryTypes[i] == TableMap.ENTRY_TYPE_FREE) {
						
						System.out.printf("@%-4d %db free\n"
								, tableMap.entryPointers[i]
								, tableMap.entryLengths[i]);
						
					} else if (tableMap.entryTypes[i] == TableMap.ENTRY_TYPE_ROWS) {
						
						Stream.iterate(tableMap.entryPointers[i], j -> j + rowSize)
							.limit(tableMap.entryLengths[i] / rowSize)
							.forEach(j -> {
								
								TableRow row = TableRow.map(buffer, j, rowSize);
								row.debugPrint();
							});
					}
				});
		});
	}

	public void clean() {
		
		tableMapStream().forEach(tableMap -> {
			
			for (int i = 0 ; i < tableMap.numberOfEntries ; i ++) {
				
				byte entryType = tableMap.entryTypes[i];
				
				if (entryType == TableMap.ENTRY_TYPE_ROWS) {
					
					while (true) {
					
						int offset = tableMap.entryPointers[i] + tableMap.entryLengths[i];
						Entry<TableMap, Integer> blockAfterMap = TableMap.getBlockAt(this, offset);
						
						if (blockAfterMap != null) {
							
							TableMap entryAfterMap = blockAfterMap.getKey();
							int entryAfterIndex = blockAfterMap.getValue();
							byte entryAfterType = entryAfterMap.entryTypes[entryAfterIndex];
							
							if (entryAfterType == TableMap.ENTRY_TYPE_ROWS) {
								
								int length = entryAfterMap.entryLengths[entryAfterIndex];
								tableMap.entryLengths[i] += length;
								entryAfterMap.entryLengths[entryAfterIndex] = 0;
//								entryAfterMap.entryPointers[entryAfterIndex] += length;
								entryAfterMap.entryTypes[entryAfterIndex] = TableMap.ENTRY_TYPE_FREE;
								entryAfterMap.writeAt(buffer, entryAfterMap.position);
								tableMap.writeAt(buffer, tableMap.position);
								continue;
							}
						}
						break;
					}
				}
			}
		});
	}

}
