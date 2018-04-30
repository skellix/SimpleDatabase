package com.skellix.database.table;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

public class ExperimentalTable {
	
	private Path directory = null;
	private Path tableFile;
	public MappedByteBuffer buffer;
	public Map<String, Integer> rowFormat;
	public Map<String, Integer> columnOffset;
	private int rowSize = 0;
	
	public ExperimentalTable(Path directory, Map<String, Integer> rowFormat) {
		
		this.directory = directory;
		
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
	
	public TableRow addRow() {
		
		int tableMapPointer = 0;
		
		while (tableMapPointer < buffer.limit()) {
			
			TableMap tableMap = TableMap.read(buffer, tableMapPointer);
			
			if (tableMap.numberOfEntries == 0) {
				
				int i = tableMap.numberOfEntries ++;
				tableMap.entryTypes[i] = TableMap.ENTRY_TYPE_ROWS;
				tableMap.entryLengths[i] += rowSize;
				
				int offset = tableMapPointer + tableMap.length();
				
				if (offset >= buffer.limit()) {
					
					offset = buffer.limit();
					resize(offset + rowSize);
					tableMap.entryPointers[i] = offset;
					TableRow row = TableRow.map(buffer, offset, rowSize);
					tableMap.writeAt(buffer, tableMapPointer);
					return row;
				}
				
				tableMap.entryPointers[i] = offset;
			}
			
			for (int i = 0 ; i < tableMap.numberOfEntries ; i ++) {
				
				int entryType = tableMap.entryTypes[i];
				int entryPointer = tableMap.entryPointers[i];
				int entryLength = tableMap.entryLengths[i];
				
				if (entryType == TableMap.ENTRY_TYPE_FREE && entryLength == 0) {
					if (entryPointer + entryLength <= buffer.limit()) {
						
						int offset = buffer.limit();
						resize(offset + rowSize);
						tableMap.entryTypes[i] = TableMap.ENTRY_TYPE_ROWS;
						tableMap.entryLengths[i] += rowSize;
						TableRow row = TableRow.map(buffer, offset, rowSize);
						tableMap.writeAt(buffer, tableMapPointer);
						return row;
					}
				}
				
				if (entryType == TableMap.ENTRY_TYPE_ROWS) {
					if (entryPointer + entryLength <= buffer.limit()) {
						
						int offset = buffer.limit();
						resize(offset + rowSize);
						tableMap.entryLengths[i] += rowSize;
						TableRow row = TableRow.map(buffer, offset, rowSize);
						tableMap.writeAt(buffer, tableMapPointer);
						return row;
					}
				}
			}
			
			if (tableMap.indexOfNextMap == 0) {
				
				// TODO: add new TableMap here
			}
		}
		
		return null;
	}
	
	private int numberOfTableMaps() {
		
		int count = 0;
		int tableMapPointer = 0;
		
		while (tableMapPointer < buffer.limit()) {
			
			TableMap tableMap = TableMap.read(buffer, tableMapPointer);
			
			count ++;
			
			if (tableMap.indexOfNextMap == 0) {
				
				break;
			}
			
			tableMapPointer = tableMap.indexOfNextMap;
		}
		
		return count;
	}
	
	private Stream<TableMap> tableMapStream() {
		
		Stream<Integer> stream = Stream.iterate(0, i -> i + 1);
		AtomicReference<Integer> tableMapPointerRef = new AtomicReference<Integer>(0);
		AtomicReference<TableMap> tableMapRef = new AtomicReference<TableMap>(null);
		return stream.limit(numberOfTableMaps()).map(i -> {
			
			TableMap tableMap = TableMap.read(buffer, tableMapPointerRef.get());
			tableMapPointerRef.set(tableMap.indexOfNextMap);
			return tableMap;
		});
	}
	
	private TableRow getRowNumber(int index) {
		
		int count = 0;
		int tableMapPointer = 0;
		
		while (tableMapPointer < buffer.limit()) {
			
			TableMap tableMap = TableMap.read(buffer, tableMapPointer);
			
			for (int i = 0 ; i < tableMap.numberOfEntries ; i ++) {
				
				int entryType = tableMap.entryTypes[i];
				int entryPointer = tableMap.entryPointers[i];
				int entryLength = tableMap.entryLengths[i];
				
				if (entryType == TableMap.ENTRY_TYPE_ROWS) {
					if (entryPointer + entryLength <= buffer.limit()) {
						
						int numEntries = entryLength / rowSize;
						
						if (index > count && index < count + numEntries) {
							
							int more = index - count;
							int offset = entryPointer + more * rowSize;
							return TableRow.map(buffer, offset, rowSize);
						}
						
						count += numEntries;
					}
				}
			}
			
			if (tableMap.indexOfNextMap == 0) {
				
				break;
			}
			
			tableMapPointer = tableMap.indexOfNextMap;
		}
		
		return null;
	}
	
	public Stream<TableRow> stream() {
		
		return tableMapStream().flatMap(tableMap -> {
			
			Stream<Integer> entryIndexStream = Stream.iterate(0, i -> i + 1);
			
			return entryIndexStream.limit(tableMap.numberOfEntries)
					.filter(i -> tableMap.entryTypes[i] == TableMap.ENTRY_TYPE_ROWS)
					.flatMap(i -> {
						
						Stream<Integer> entryStream = Stream.iterate(tableMap.entryPointers[i], j -> j + rowSize);
						
						return entryStream.limit(tableMap.entryLengths[i] / rowSize).map(j -> {
							
							return TableRow.map(buffer, j, rowSize);
						});
					});
		});
	}

	public int rowCount() {
		
		int count = 0;
		
		int tableMapPointer = 0;
		
		while (tableMapPointer < buffer.limit()) {
			
			TableMap tableMap = TableMap.read(buffer, tableMapPointer);
			
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
	
	private void initTable() {
		
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
		
		if (this.directory == null) {
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

}
