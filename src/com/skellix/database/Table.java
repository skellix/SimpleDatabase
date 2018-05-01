package com.skellix.database;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel.MapMode;
import java.nio.channels.FileLock;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;

/**
 * 
 * @author Alexander Jones <24k911@gmail.com"> 
 *
 */
public class Table {

	private File directory = null;
	private Map<String, Integer> rowFormat;
	private int rowSize = 0;
	private TableColumn size = null;
	
	public Table(File directory, Map<String, Integer> rowFormat) {
		
		this.directory = directory;
		
		if (this.directory == null) {
			System.err.printf("The init method for %s must initialize the directory field\n", this.getClass().getCanonicalName());
			System.exit(-1);
		}
		
		if (!this.directory.exists()) {
			this.directory.mkdirs();
		}
		
		File sizeFile = new File(this.directory, "_size");
		
		if (!sizeFile.exists()) {
			try {
				sizeFile.createNewFile();
				OutputStream outputStream = new FileOutputStream(sizeFile);
				
				outputStream.write(new byte[Long.BYTES]);
				
				outputStream.close();
				
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			try {
				RandomAccessFile randomAccessFile = new RandomAccessFile(sizeFile, "rw");
				
				size = new TableColumn(Long.BYTES, randomAccessFile.getChannel().map(MapMode.READ_WRITE, 0, Long.BYTES));
				
				randomAccessFile.close();
				
				synchronized (size) {
					size.setLong(0);
				}
				
			} catch (IOException e) {
				e.printStackTrace();
			}
			
		} else {
			
			try {
				RandomAccessFile randomAccessFile = new RandomAccessFile(sizeFile, "rw");
				
				size = new TableColumn(Long.BYTES, randomAccessFile.getChannel().map(MapMode.READ_WRITE, 0, Long.BYTES));
				
				randomAccessFile.close();
				
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		this.rowFormat = rowFormat;
		for (String key : rowFormat.keySet()) {
			rowSize += rowFormat.get(key);
		}
	}
	
	public Stream<TableColumn[]> stream() {
		
		Stream<Integer> stream = Stream.iterate(0, i -> i + 1);
		return stream.limit(getTableSize()).map(i -> getRow(i));
	}
	
	public File getDirectory() {
		return directory;
	}
	
	public int getRowSize() {
		return rowSize;
	}
	
	public long getTableSize() {
		synchronized (size) {
			return size.getLong();
		}
	}
	
	public Integer getColumnIndex(String columnName) {
		
		int i = 0;
		
		for (Entry<String, Integer> column : getRowFormat().entrySet()) {
			
			if (column.getKey().equals(columnName)) {
				
				return i;
			}
			
			i ++;
		}
		
		return null;
	}
	
	public HashMap<String, Integer> getRowFormat() {
		return new HashMap<String, Integer>(rowFormat);
	}
	
	private File safeRowGet(long index) {
		
		while (index > getTableSize()) {
			safeRowGet(getTableSize());
		}
		
		File rowFile = new File(directory, String.format("%d", index));
		
		if (!rowFile.exists()) {
			try {
				
				rowFile.createNewFile();
				OutputStream outputStream = new FileOutputStream(rowFile);
				
				outputStream.write(new byte[getRowSize()]);
				
				outputStream.flush();
				
				outputStream.close();
				
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		if (index == getTableSize()) {
			synchronized (size) {
				size.setLong(getTableSize()+ 1);
			}
		}
		
		return rowFile;
	}
	
	/***
	 * Method for easily adding a row to the table
	 * @return The row added
	 */
	public TableColumn[] addRow() {
		
		return getRow(getTableSize());
	}
	
	/***
	 * Retrieve a row from the table by index
	 * @param index of the row
	 * @return the row at the specified index
	 */
	public TableColumn[] getRow(long index) {
		
		File rowFile = safeRowGet(index);
		
		try {
			RandomAccessFile randomAccessFile = new RandomAccessFile(rowFile, "rw");
			
			String[] columnsNames = rowFormat.keySet().toArray(new String[0]);
			TableColumn[] columns = new TableColumn[rowFormat.size()];
			
			long offset = 0;
			for (int i = 0 ; i < columns.length ; i ++) {
				
				int columnSize = rowFormat.get(columnsNames[i]);
				columns[i] = new TableColumn(columnSize, randomAccessFile.getChannel().map(MapMode.READ_WRITE, offset, columnSize));
				offset += columnSize;
			}
			
			randomAccessFile.close();
			
			return columns;
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return null;
	}
	
	public HashMap<String, TableColumn> getRowMap(long index) {
		
		File rowFile = safeRowGet(index);
		
		try {
			RandomAccessFile randomAccessFile = new RandomAccessFile(rowFile, "rw");
			
			String[] columnsNames = rowFormat.keySet().toArray(new String[0]);
			HashMap<String, TableColumn> columns = new HashMap<String, TableColumn>();
			
			long offset = 0;
			for (int i = 0 ; i < columnsNames.length ; i ++) {
				
				int columnSize = rowFormat.get(columnsNames[i]);
				columns.put(columnsNames[i], new TableColumn(columnSize, randomAccessFile.getChannel().map(MapMode.READ_WRITE, offset, columnSize)));
				
				offset += columnSize;
			}
			
			randomAccessFile.close();
			
			return columns;
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return null;
	}
	
	public FileLock lockRow(long index) {
		
		File rowFile = safeRowGet(index);
		
		try (RandomAccessFile randomAccessFile = new RandomAccessFile(rowFile, "rw")) {
			
			FileLock lock = null;
			
			while ((lock = randomAccessFile.getChannel().tryLock(0, getRowSize(), false)) == null) {
				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			
			//randomAccessFile.close();
			
			return lock;
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return null;
	}
}
