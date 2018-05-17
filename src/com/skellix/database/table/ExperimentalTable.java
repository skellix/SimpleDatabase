package com.skellix.database.table;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

import com.skellix.database.row.RowFormat;
import com.skellix.database.row.RowFormatter;
import com.skellix.database.row.RowFormatterException;
import com.skellix.database.row.TableRow;
import com.skellix.database.session.Session;

public class ExperimentalTable {
	
	public static Map<String, ExperimentalTable> openTables = new LinkedHashMap<>();
	
	private Path tableFile;
	private Path tableMapFile;
	public MappedByteBuffer buffer;
	public MappedByteBuffer tableMapBuffer;
	public RowFormat rowFormat;
	
	private ReadWriteLock locker = new ReentrantReadWriteLock(true);
	
	protected ExperimentalTable() {
		
		//
	}
	
	private ExperimentalTable(Path directory, RowFormat rowFormat) {
		
		synchronized (openTables) {
			
			this.rowFormat = rowFormat;
			
			initDir(directory);
			
			try {
				rowFormat.write(getFormatPath(directory));
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			
			tableFile = getRowDataPath(directory);
			
			try (RandomAccessFile randomAccessFile = new RandomAccessFile(tableFile.toFile(), "rw")) {
				
				buffer = randomAccessFile.getChannel().map(MapMode.READ_WRITE, 0, Files.size(tableFile));
				
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			tableMapFile = getTableMapPath(directory);
			
			try (RandomAccessFile randomAccessFile = new RandomAccessFile(tableMapFile.toFile(), "rw")) {
				
				tableMapBuffer = randomAccessFile.getChannel().map(MapMode.READ_WRITE, 0, Files.size(tableMapFile));
				
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			if (tableMapBuffer.limit() == 0) {
				
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
	
	public String getName() {
		
		return tableFile.getParent().getFileName().toString();
	}
	
	public static Path getRowDataPath(Path directory) {
		
		return directory.resolve("table.bin");
	}
	
	private static Path getTableMapPath(Path directory) {
		
		return directory.resolve("map.bin");
	}
	
	public static Path getFormatPath(Path directory) {
		
		return directory.resolve("format");
	}

	public void deleteTable() throws IOException {
		
		Lock writeLock = locker.writeLock();
		writeLock.lock();
		
		TableMap.removeMapsForTable(this);
		
		closeBuffers();
		resize(0);
		resizeTableMap(0);
		deleteFiles(tableFile.getParent());
		openTables.remove(uid());
		
		writeLock.unlock();
	}
	
	public static void deleteTable(Path tablePath) throws IOException {
		
		
		String uid = ExperimentalTable.uid(tablePath);
		ExperimentalTable table = openTables.get(uid);
		
		if (table == null) {
			
			deleteFiles(tablePath);
			
		} else {
			
			table.deleteTable();
		}
	}
	
	public static void deleteFiles(Path tablePath) throws IOException {
		
		Path rowDataPath = getRowDataPath(tablePath);
		
		if (Files.exists(rowDataPath)) {
			
			System.out.println("deleting: " + rowDataPath.toAbsolutePath().toString());
			Files.delete(rowDataPath);
//			System.out.println("deleted: " + rowDataPath.toAbsolutePath().toString());
		}
		
		Path tableMapFile = getTableMapPath(tablePath);
		if (Files.exists(tableMapFile)) {
			
			System.out.println("deleting: " + tableMapFile.toAbsolutePath().toString());
			Files.delete(tableMapFile);
//			System.out.println("deleted: " + rowDataPath.toAbsolutePath().toString());
		}
		
		Path formatPath = getFormatPath(tablePath);
		
		if (Files.exists(formatPath)) {
			
			System.out.println("deleting: " + formatPath.toAbsolutePath().toString());
			Files.delete(formatPath);
//			System.out.println("deleted: " + formatPath.toAbsolutePath().toString());
		}
		
		Path parent = tablePath;
		
		if (Files.exists(parent)) {
			
			System.out.println("deleting: " + parent.toAbsolutePath().toString());
			Files.delete(parent);
//			System.out.println("deleted: " + parent.toAbsolutePath().toString());
		}
	}
	
	private void closeBuffers() {
		
		closeBuffer(buffer);
		closeBuffer(tableMapBuffer);
	}
	
	private void closeBuffer(MappedByteBuffer buffer) {
		try {
			
			Field cleanerField = buffer.getClass().getDeclaredField("cleaner");
			boolean accessible = cleanerField.isAccessible();
			cleanerField.setAccessible(true);
			Object cleaner = cleanerField.get(buffer);
			
			if (cleaner == null) {
				
				// not opened yet
				return;
			}
			
			Method method = cleaner.getClass().getDeclaredMethod("clean");
			method.invoke(cleaner);
			cleanerField.setAccessible(accessible);
			
		} catch (NoSuchFieldException e1) {
			e1.printStackTrace();
			System.exit(-1);
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
			System.exit(-1);
		} catch (IllegalAccessException e) {
			e.printStackTrace();
			System.exit(-1);
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
			System.exit(-1);
		} catch (SecurityException e) {
			e.printStackTrace();
			System.exit(-1);
		} catch (InvocationTargetException e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}
	
	private Path getFormatPath() {
		
		return getFormatPath(tableFile.getParent());
	}

	public Lock getReadLock() {
		
		return locker.readLock();
	}
	
	public Lock getWriteLock() {
		
		return locker.writeLock();
	}
	
	public TableRow addRow(Session session) throws Exception {
		
		if (!session.hasWritePermission()) {
			
			throw new Exception("Write permission is required to insert a row");
		}
		
		TableMap tableMap = TableMap.read(this, 0);
		return tableMap.addRow(this, rowFormat.rowSize);
//		
//		while (tableMapPointer < tableMapBuffer.limit()) {
//			
//			TableMap tableMap = TableMap.read(this, tableMapPointer);
//			
//			if (tableMap.numberOfEntries == 0) {
//				
//				int offset = buffer.limit();
//				resize(offset + rowFormat.rowSize);
//				TableMap.addEntry(this, TableMap.ENTRY_TYPE_ROWS, offset, rowFormat.rowSize);
//				return TableRow.map(this, offset, rowFormat.rowSize);
//			}
//			
//			for (int i = 0 ; i < tableMap.numberOfEntries ; i ++) {
//				
//				int entryType = tableMap.entryTypes[i];
//				int entryPointer = tableMap.entryPointers[i];
//				
//				if (entryType == TableMap.ENTRY_TYPE_FREE) {
//					
//					if (tableMap.entryLengths[i] >= rowFormat.rowSize) {
//						
//						Entry<TableMap, Integer> tableMapBefore = TableMap.getBlockBefore(this, entryPointer);
//						
//						if (tableMapBefore != null) {
//							
//							TableMap mapBefore = tableMapBefore.getKey();
//							Integer entryBeforeIndex = tableMapBefore.getValue();
//							byte entryBeforeType = mapBefore.entryTypes[entryBeforeIndex];
//							
//							if (entryBeforeType == TableMap.ENTRY_TYPE_ROWS) {
//								
//								int offset = mapBefore.entryPointers[entryBeforeIndex] + mapBefore.entryLengths[entryBeforeIndex];
//								mapBefore.entryLengths[entryBeforeIndex] += rowFormat.rowSize;
//								tableMap.entryPointers[i] += rowFormat.rowSize;
//								tableMap.entryLengths[i] -= rowFormat.rowSize;
//								mapBefore.writeAt(tableMapBuffer, mapBefore.position);
//								tableMap.writeAt(tableMapBuffer, tableMap.position);
//								return TableRow.map(this, offset, rowFormat.rowSize);
//							}
//						}
//						
//						Entry<TableMap, Integer> tableMapAfter = TableMap.getBlockAt(this, entryPointer + tableMap.entryLengths[i]);
//						
//						if (tableMapAfter != null) {
//							
//							TableMap mapAfter = tableMapAfter.getKey();
//							Integer entryAfterIndex = tableMapAfter.getValue();
//							byte entryAfterType = mapAfter.entryTypes[entryAfterIndex];
//							
//							if (entryAfterType == TableMap.ENTRY_TYPE_ROWS) {
//								
//								int offset = mapAfter.entryPointers[entryAfterIndex];
//								mapAfter.entryPointers[entryAfterIndex] += rowFormat.rowSize;
//								mapAfter.entryLengths[entryAfterIndex] -= rowFormat.rowSize;
//								tableMap.entryLengths[i] -= rowFormat.rowSize;
//								mapAfter.writeAt(tableMapBuffer, mapAfter.position);
//								tableMap.writeAt(tableMapBuffer, tableMap.position);
//								return TableRow.map(this, offset, rowFormat.rowSize);
//							}
//						}
//					}
//				} else if (entryType == TableMap.ENTRY_TYPE_ROWS) {
//					
//					if (entryPointer + tableMap.entryLengths[i] == tableMapBuffer.limit()) {
//						
//						int offset = tableMapBuffer.limit();
//						resize(offset + rowFormat.rowSize);
//						tableMap.entryLengths[i] += rowFormat.rowSize;
//						tableMap.writeAt(tableMapBuffer, tableMap.position);
//						return TableRow.map(this, offset, rowFormat.rowSize);
//					}
//					
//					Entry<TableMap, Integer> tableMapBefore = TableMap.getBlockBefore(this, entryPointer);
//					
//					if (tableMapBefore != null) {
//						
//						TableMap mapBefore = tableMapBefore.getKey();
//						Integer entryBeforeIndex = tableMapBefore.getValue();
//						byte entryBeforeType = mapBefore.entryTypes[entryBeforeIndex];
//						
//						if (entryBeforeType == TableMap.ENTRY_TYPE_FREE) {
//							if (mapBefore.entryLengths[entryBeforeIndex] >= rowFormat.rowSize) {
//								
//								int offset = entryPointer - rowFormat.rowSize;
//								mapBefore.entryLengths[entryBeforeIndex] -= rowFormat.rowSize;
//								tableMap.entryPointers[i] -= rowFormat.rowSize;
//								tableMap.entryLengths[i] += rowFormat.rowSize;
//								mapBefore.writeAt(tableMapBuffer, mapBefore.position);
//								tableMap.writeAt(tableMapBuffer, tableMap.position);
//								return TableRow.map(this, offset, rowFormat.rowSize);
//							}
//						}
//					}
//					
//					Entry<TableMap, Integer> tableMapAfter = TableMap.getBlockAt(this, entryPointer + tableMap.entryLengths[i]);
//					
//					if (tableMapAfter != null) {
//						
//						TableMap mapAfter = tableMapAfter.getKey();
//						Integer entryAfterIndex = tableMapAfter.getValue();
//						byte entryAfterType = mapAfter.entryTypes[entryAfterIndex];
//						
//						if (entryAfterType == TableMap.ENTRY_TYPE_FREE) {
//							if (mapAfter.entryLengths[entryAfterIndex] >= rowFormat.rowSize) {
//								
//								int offset = mapAfter.entryPointers[entryAfterIndex];
//								mapAfter.entryPointers[entryAfterIndex] += rowFormat.rowSize;
//								mapAfter.entryLengths[entryAfterIndex] -= rowFormat.rowSize;
//								tableMap.entryLengths[i] += rowFormat.rowSize;
//								mapAfter.writeAt(tableMapBuffer, mapAfter.position);
//								tableMap.writeAt(tableMapBuffer, tableMap.position);
//								return TableRow.map(this, offset, rowFormat.rowSize);
//							}
//						}
//					}
//				}
//			}
//			
//			if (tableMap.indexOfNextMap == 0) {
//				
//				TableMap nextMap = new TableMap();
//				int offset = tableMapBuffer.limit();
//				resize(offset + nextMap.length());
//				nextMap.writeAt(tableMapBuffer, offset);
//				offset = tableMapBuffer.limit();
//				resize(offset + rowFormat.rowSize);
//				tableMap.indexOfNextMap = nextMap.position;
//				tableMap.writeAt(tableMapBuffer, tableMap.position);
//				TableMap.addEntry(this, TableMap.ENTRY_TYPE_ROWS, offset, rowFormat.rowSize);
//				return TableRow.map(this, offset, rowFormat.rowSize);
//			}
//		}
//		
//		return null;
	}
	
	public String uid() {
		
		return tableFile.toAbsolutePath().toString();
	}
	
	public static String uid(Path directory) {
		
		return getRowDataPath(directory).toAbsolutePath().toString();
	}
	
//	private int numberOfTableMaps() {
//		
//		int count = 0;
//		int tableMapPointer = 0;
//		
//		while (tableMapPointer < tableMapBuffer.limit()) {
//			
//			TableMap tableMap = TableMap.read(this, tableMapPointer);
//			
//			count ++;
//			
//			if (tableMap.indexOfNextMap == 0) {
//				
//				break;
//			}
//			
//			tableMapPointer = tableMap.indexOfNextMap;
//		}
//		
//		return count;
//	}
//	
//	public Stream<TableMap> tableMapStream() {
//		
//		Stream<Integer> stream = Stream.iterate(0, i -> i + 1);
//		AtomicReference<Integer> tableMapPointerRef = new AtomicReference<Integer>(0);
//		return stream.limit(numberOfTableMaps()).map(i -> {
//			
//			TableMap tableMap = TableMap.read(this, tableMapPointerRef.get());
//			tableMapPointerRef.set(tableMap.indexOfNextMap);
//			return tableMap;
//		});
//	}
	
	public void deleteRow(TableRow rowToDelete, Session session) throws Exception {
		
		if (!session.hasWritePermission()) {
			
			throw new Exception("Write permission is required to delete a row");
		}
		
		TableMap tableMap = TableMap.read(this, 0);
		
		int rowOffset = rowToDelete.offset;
		tableMap.free(rowOffset, rowFormat.rowSize);
//		
//		int tableMapPointer = 0;
//		
//		while (tableMapPointer < tableMapBuffer.limit()) {
//			
//			TableMap tableMap = TableMap.read(this, tableMapPointer);
//			
//			for (int i = 0 ; i < tableMap.numberOfEntries ; i ++) {
//				
//				int entryType = tableMap.entryTypes[i];
//				int entryPointer = tableMap.entryPointers[i];
//				int entryLength = tableMap.entryLengths[i];
//				
//				if (entryType == TableMap.ENTRY_TYPE_ROWS) {
//					if (entryPointer + entryLength <= tableMapBuffer.limit()) {
//						
//						if (rowOffset > entryPointer && rowOffset < entryPointer + entryLength) {
//							
//							tableMap.removeData(i, rowOffset, rowFormat.rowSize, this);
//							return;
//						}
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
	}
	
	public Stream<TableRow> stream() {
		
		TableMap tableMap = TableMap.read(this, 0);
		
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
//		
//		return tableMapStream().flatMap(tableMap -> {
//			
//			Stream<Integer> entryIndexStream = Stream.iterate(0, i -> i + 1);
//			
//			return entryIndexStream.limit(tableMap.numberOfEntries)
//					.filter(i -> tableMap.entryTypes[i] == TableMap.ENTRY_TYPE_ROWS)
//					.flatMap(i -> {
//						
//						Stream<Integer> entryStream = Stream.iterate(tableMap.entryPointers[i], j -> j + rowFormat.rowSize);
//						
//						return entryStream.limit(tableMap.entryLengths[i] / rowFormat.rowSize)
//								.map(j -> {
//							
//									return TableRow.map(this, j, rowFormat.rowSize);
//								});
//					});
//		});
	}

	public int rowCount() {
		
		int count = 0;
		TableMap tableMap = TableMap.read(this, 0);
		
		for (int i = 0 ; i < tableMap.numberOfEntries ; i ++) {
			
			int entryType = tableMap.entryTypes[i];
			int entryPointer = tableMap.entryPointers[i];
			int entryLength = tableMap.entryLengths[i];
			
			if (entryType == TableMap.ENTRY_TYPE_ROWS) {
				if (entryPointer + entryLength <= tableMapBuffer.limit()) {
					
					count += entryLength / rowFormat.rowSize;
				}
			}
		}
//		
//		int tableMapPointer = 0;
//		
//		while (tableMapPointer < tableMapBuffer.limit()) {
//			
//			TableMap tableMap = TableMap.read(this, tableMapPointer);
//			
//			for (int i = 0 ; i < tableMap.numberOfEntries ; i ++) {
//				
//				int entryType = tableMap.entryTypes[i];
//				int entryPointer = tableMap.entryPointers[i];
//				int entryLength = tableMap.entryLengths[i];
//				
//				if (entryType == TableMap.ENTRY_TYPE_ROWS) {
//					if (entryPointer + entryLength <= tableMapBuffer.limit()) {
//						
//						count += entryLength / rowFormat.rowSize;
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
		return count;
	}
	
	public void initTable() {
		
		Path directory = tableFile.getParent();
		
		initDir(directory);
		
		try {
			rowFormat.write(getFormatPath(directory));
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
		TableMap tableMap = new TableMap();
		resizeTableMap(tableMap.length());
		tableMap.writeAt(tableMapBuffer, 0);
	}

	public void resizeTableMap(int size) {
		
		tableMapBuffer = resize(tableMapBuffer, tableMapFile, size);
	}

	public void resize(int size) {
		
		buffer = resize(buffer, tableFile, size);
	}
	
	public static MappedByteBuffer resize(MappedByteBuffer buffer, Path path, int size) {
		
		try (RandomAccessFile randomAccessFile = new RandomAccessFile(path.toFile(), "rw")) {
			
			long oldSize = Files.size(path);
			int diff = (int) (size - oldSize);
			randomAccessFile.setLength(size);
			buffer = randomAccessFile.getChannel().map(MapMode.READ_WRITE, 0, Files.size(path));
			
			if (size > oldSize) {
				
				buffer.position((int) oldSize);
				buffer.put(new byte[diff]);
			}
			
			return buffer;
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return null;
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
		
		TableMap tableMap = TableMap.read(this, 0);
		
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
//		
//		tableMapStream().forEach(tableMap -> {
//			
//			tableMap.debugPrint();
//			
//			Stream.iterate(0, i -> i + 1)
//				.limit(tableMap.numberOfEntries)
//				.forEach(i -> {
//					
//					if (tableMap.entryTypes[i] == TableMap.ENTRY_TYPE_FREE) {
//						
//						System.out.printf("@%-4d %db free\n"
//								, tableMap.entryPointers[i]
//								, tableMap.entryLengths[i]);
//						
//					} else if (tableMap.entryTypes[i] == TableMap.ENTRY_TYPE_ROWS) {
//						
//						Stream.iterate(tableMap.entryPointers[i], j -> j + rowFormat.rowSize)
//							.limit(tableMap.entryLengths[i] / rowFormat.rowSize)
//							.forEach(j -> {
//								
//								TableRow row = TableRow.map(this, j, rowFormat.rowSize);
//								row.debugPrint();
//							});
//					}
//				});
//		});
	}

	public void clean() {
		
//		tableMapStream().forEach(tableMap -> {
//			
//			for (int i = 0 ; i < tableMap.numberOfEntries ; i ++) {
//				
//				byte entryType = tableMap.entryTypes[i];
//				
//				if (entryType == TableMap.ENTRY_TYPE_ROWS) {
//					
//					while (true) {
//					
//						int offset = tableMap.entryPointers[i] + tableMap.entryLengths[i];
//						Entry<TableMap, Integer> blockAfterMap = TableMap.getBlockAt(this, offset);
//						
//						if (blockAfterMap != null) {
//							
//							TableMap entryAfterMap = blockAfterMap.getKey();
//							int entryAfterIndex = blockAfterMap.getValue();
//							byte entryAfterType = entryAfterMap.entryTypes[entryAfterIndex];
//							
//							if (entryAfterType == TableMap.ENTRY_TYPE_ROWS) {
//								
//								int length = entryAfterMap.entryLengths[entryAfterIndex];
//								tableMap.entryLengths[i] += length;
//								entryAfterMap.entryLengths[entryAfterIndex] = 0;
////								entryAfterMap.entryPointers[entryAfterIndex] += length;
//								entryAfterMap.entryTypes[entryAfterIndex] = TableMap.ENTRY_TYPE_FREE;
//								entryAfterMap.writeAt(tableMapBuffer, entryAfterMap.position);
//								tableMap.writeAt(tableMapBuffer, tableMap.position);
//								continue;
//							}
//						}
//						break;
//					}
//				}
//			}
//		});
	}

	public void printAllRows(TableFormat tableFormat) {
		
		TableFormatter.printTableStart(this, tableFormat);
		stream().forEach(row -> TableFormatter.printTableRow(this, row, tableFormat));
		TableFormatter.printTableEnd(this, tableFormat);
	}

}
