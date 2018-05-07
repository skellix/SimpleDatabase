package com.skellix.database.table;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

public class ExperimentalTable {
	
	public static Map<String, ExperimentalTable> openTables = new LinkedHashMap<>();
	
	private Path tableFile;
	public MappedByteBuffer buffer;
	public RowFormat rowFormat;
	
	private ReadWriteLock locker = new ReentrantReadWriteLock(true);
	
	private ExperimentalTable(Path directory, RowFormat rowFormat) {
		
		synchronized (openTables) {
			
			this.rowFormat = rowFormat;
			Path rowFormatPath = getFormatPath(directory);
			
			try {
				rowFormat.write(rowFormatPath);
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			
			initDir(directory);
			
			tableFile = getRowDataPath(directory);
			
			try (RandomAccessFile randomAccessFile = new RandomAccessFile(tableFile.toFile(), "rw")) {
				
				buffer = randomAccessFile.getChannel().map(MapMode.READ_WRITE, 0, Files.size(tableFile));
				
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			if (buffer.limit() == 0) {
				
				initTable();
			}
			
			openTables.put(uid(), this);
		}
	}
	
	public static ExperimentalTable getOrCreate(Path directory, RowFormat rowFormat) {
		
		String uid = uid(directory);
		
		synchronized (openTables) {
			
			if (openTables.containsKey(uid)) {
				
				return openTables.get(uid);
			}
			
			return new ExperimentalTable(directory, rowFormat);
		}
	}
	
	public static ExperimentalTable getById(String tableIdString) throws FileNotFoundException,RowFormatterException {
		
		Path directory = Paths.get(tableIdString);
		
		if (!Files.exists(directory)) {
			
			throw new FileNotFoundException("No database found at the path " + directory.toAbsolutePath().toString());
		}
		
		String uid = uid(directory);
		
		synchronized (openTables) {
			
			if (openTables.containsKey(uid)) {
				
				return openTables.get(uid);
			}
		}
		
		Path format = getFormatPath(directory);
		
		if (!Files.exists(format)) {
			
			throw new FileNotFoundException("No database format found at the path " + format.toAbsolutePath().toString());
		}
		
		RowFormat rowFormat = null;
		try {
			
			rowFormat = RowFormatter.parse(format);
			
		} catch (IOException e) {
			throw new RowFormatterException(e.getMessage());
		} catch (RowFormatterException e) {
			throw e;
		}
		
		return new ExperimentalTable(directory, rowFormat);
	}
	
	public static Path getRowDataPath(Path directory) {
		
		return directory.resolve("table.bin");
	}
	
	public static Path getFormatPath(Path directory) {
		
		return directory.resolve("format");
	}

	public void deleteTable() throws IOException {
		
		System.out.println("deleting: " + tableFile.toAbsolutePath().toString());
		Files.delete(tableFile);
		System.out.println("deleted: " + tableFile.toAbsolutePath().toString());
	}
	
	public Lock getReadLock() {
		
		return locker.readLock();
	}
	
	public Lock getWriteLock() {
		
		return locker.writeLock();
	}
	
	public TableRow addRow() {
		
		int tableMapPointer = 0;
		
		while (tableMapPointer < buffer.limit()) {
			
			TableMap tableMap = TableMap.read(this, tableMapPointer);
			
			if (tableMap.numberOfEntries == 0) {
				
				int offset = buffer.limit();
				resize(offset + rowFormat.rowSize);
				TableMap.addEntry(this, TableMap.ENTRY_TYPE_ROWS, offset, rowFormat.rowSize);
				return TableRow.map(this, offset, rowFormat.rowSize);
			}
			
			for (int i = 0 ; i < tableMap.numberOfEntries ; i ++) {
				
				int entryType = tableMap.entryTypes[i];
				int entryPointer = tableMap.entryPointers[i];
				
				if (entryType == TableMap.ENTRY_TYPE_FREE) {
					if (tableMap.entryLengths[i] >= rowFormat.rowSize) {
						
						Entry<TableMap, Integer> tableMapBefore = TableMap.getBlockBefore(this, entryPointer);
						
						if (tableMapBefore != null) {
							
							TableMap mapBefore = tableMapBefore.getKey();
							Integer entryBeforeIndex = tableMapBefore.getValue();
							byte entryBeforeType = mapBefore.entryTypes[entryBeforeIndex];
							
							if (entryBeforeType == TableMap.ENTRY_TYPE_ROWS) {
								
								int offset = mapBefore.entryPointers[entryBeforeIndex] + mapBefore.entryLengths[entryBeforeIndex];
								mapBefore.entryLengths[entryBeforeIndex] += rowFormat.rowSize;
								tableMap.entryPointers[i] += rowFormat.rowSize;
								tableMap.entryLengths[i] -= rowFormat.rowSize;
								mapBefore.writeAt(buffer, mapBefore.position);
								tableMap.writeAt(buffer, tableMap.position);
								return TableRow.map(this, offset, rowFormat.rowSize);
							}
						}
						
						Entry<TableMap, Integer> tableMapAfter = TableMap.getBlockAt(this, entryPointer + tableMap.entryLengths[i]);
						
						if (tableMapAfter != null) {
							
							TableMap mapAfter = tableMapAfter.getKey();
							Integer entryAfterIndex = tableMapAfter.getValue();
							byte entryAfterType = mapAfter.entryTypes[entryAfterIndex];
							
							if (entryAfterType == TableMap.ENTRY_TYPE_ROWS) {
								
								int offset = mapAfter.entryPointers[entryAfterIndex];
								mapAfter.entryPointers[entryAfterIndex] += rowFormat.rowSize;
								mapAfter.entryLengths[entryAfterIndex] -= rowFormat.rowSize;
								tableMap.entryLengths[i] -= rowFormat.rowSize;
								mapAfter.writeAt(buffer, mapAfter.position);
								tableMap.writeAt(buffer, tableMap.position);
								return TableRow.map(this, offset, rowFormat.rowSize);
							}
						}
					}
				} else if (entryType == TableMap.ENTRY_TYPE_ROWS) {
					
					if (entryPointer + tableMap.entryLengths[i] == buffer.limit()) {
						
						int offset = buffer.limit();
						resize(offset + rowFormat.rowSize);
						tableMap.entryLengths[i] += rowFormat.rowSize;
						tableMap.writeAt(buffer, tableMap.position);
						return TableRow.map(this, offset, rowFormat.rowSize);
					}
					
					Entry<TableMap, Integer> tableMapBefore = TableMap.getBlockBefore(this, entryPointer);
					
					if (tableMapBefore != null) {
						
						TableMap mapBefore = tableMapBefore.getKey();
						Integer entryBeforeIndex = tableMapBefore.getValue();
						byte entryBeforeType = mapBefore.entryTypes[entryBeforeIndex];
						
						if (entryBeforeType == TableMap.ENTRY_TYPE_FREE) {
							if (mapBefore.entryLengths[entryBeforeIndex] >= rowFormat.rowSize) {
								
								int offset = entryPointer - rowFormat.rowSize;
								mapBefore.entryLengths[entryBeforeIndex] -= rowFormat.rowSize;
								tableMap.entryPointers[i] -= rowFormat.rowSize;
								tableMap.entryLengths[i] += rowFormat.rowSize;
								mapBefore.writeAt(buffer, mapBefore.position);
								tableMap.writeAt(buffer, tableMap.position);
								return TableRow.map(this, offset, rowFormat.rowSize);
							}
						}
					}
					
					Entry<TableMap, Integer> tableMapAfter = TableMap.getBlockAt(this, entryPointer + tableMap.entryLengths[i]);
					
					if (tableMapAfter != null) {
						
						TableMap mapAfter = tableMapAfter.getKey();
						Integer entryAfterIndex = tableMapAfter.getValue();
						byte entryAfterType = mapAfter.entryTypes[entryAfterIndex];
						
						if (entryAfterType == TableMap.ENTRY_TYPE_FREE) {
							if (mapAfter.entryLengths[entryAfterIndex] >= rowFormat.rowSize) {
								
								int offset = mapAfter.entryPointers[entryAfterIndex];
								mapAfter.entryPointers[entryAfterIndex] += rowFormat.rowSize;
								mapAfter.entryLengths[entryAfterIndex] -= rowFormat.rowSize;
								tableMap.entryLengths[i] += rowFormat.rowSize;
								mapAfter.writeAt(buffer, mapAfter.position);
								tableMap.writeAt(buffer, tableMap.position);
								return TableRow.map(this, offset, rowFormat.rowSize);
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
				resize(offset + rowFormat.rowSize);
				tableMap.indexOfNextMap = nextMap.position;
				tableMap.writeAt(buffer, tableMap.position);
				TableMap.addEntry(this, TableMap.ENTRY_TYPE_ROWS, offset, rowFormat.rowSize);
				return TableRow.map(this, offset, rowFormat.rowSize);
			}
		}
		
		return null;
	}
	
	public String uid() {
		
		return tableFile.toAbsolutePath().toString();
	}
	
	public static String uid(Path directory) {
		
		return getRowDataPath(directory).toAbsolutePath().toString();
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
							
							tableMap.removeData(i, rowOffset, rowFormat.rowSize, this);
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
	
	public Stream<TableRow> stream() {
		
		return tableMapStream().flatMap(tableMap -> {
			
			Stream<Integer> entryIndexStream = Stream.iterate(0, i -> i + 1);
			
			return entryIndexStream.limit(tableMap.numberOfEntries)
					.filter(i -> tableMap.entryTypes[i] == TableMap.ENTRY_TYPE_ROWS)
					.flatMap(i -> {
						
						Stream<Integer> entryStream = Stream.iterate(tableMap.entryPointers[i], j -> j + rowFormat.rowSize);
						
						return entryStream.limit(tableMap.entryLengths[i] / rowFormat.rowSize)
								.map(j -> {
							
									return TableRow.map(this, j, rowFormat.rowSize);
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
						
						count += entryLength / rowFormat.rowSize;
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
		
		return rowFormat.columnNames.toArray(new String[0]);
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
						
						Stream.iterate(tableMap.entryPointers[i], j -> j + rowFormat.rowSize)
							.limit(tableMap.entryLengths[i] / rowFormat.rowSize)
							.forEach(j -> {
								
								TableRow row = TableRow.map(this, j, rowFormat.rowSize);
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

	public void printAllRows(TableFormat tableFormat) {
		
		TableFormatter.printTableStart(this, tableFormat);
		stream().forEach(row -> TableFormatter.printTableRow(this, row, tableFormat));
		TableFormatter.printTableEnd(this, tableFormat);
	}

}
