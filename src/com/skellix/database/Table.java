package com.skellix.database;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel.MapMode;
import java.nio.channels.FileLock;
import java.util.HashMap;

public class Table {

	private File directory = null;
	private HashMap<String, Integer> rowFormat;
	private int rowSize = 0;
	private TableColumn size = null;
	
	public Table(File directory, HashMap<String, Integer> rowFormat) {
		
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
				
				outputStream.write(new byte[8]);
				
				outputStream.close();
				
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		try {
			RandomAccessFile randomAccessFile = new RandomAccessFile(sizeFile, "rw");
			
			size = new TableColumn(8, randomAccessFile.getChannel().map(MapMode.READ_WRITE, 0, 8));
			
			randomAccessFile.close();
			
			synchronized (size) {
				size.setInt(0);
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		this.rowFormat = rowFormat;
		for (String key : rowFormat.keySet()) {
			rowSize += rowFormat.get(key);
		}
	}
	
	public File getDirectory() {
		return directory;
	}
	
	public int getRowSize() {
		return rowSize;
	}
	
	public int getTableSize() {
		synchronized (size) {
			return size.getInt();
		}
	}
	
	public HashMap<String, Integer> getRowFormat() {
		return new HashMap<String, Integer>(rowFormat);
	}
	
	private File safeRowGet(int index) {
		
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
				size.setInt(getTableSize()+ 1);
			}
		}
		
		return rowFile;
	}
	
	public TableColumn[] getRow(int index) {
		
		File rowFile = safeRowGet(index);
		
		try {
			RandomAccessFile randomAccessFile = new RandomAccessFile(rowFile, "rw");
			
			String[] columnsNames = rowFormat.keySet().toArray(new String[0]);
			TableColumn[] columns = new TableColumn[rowFormat.size()];
			
			int offset = 0;
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
	
	public HashMap<String, TableColumn> getRowMap(int index) {
		
		File rowFile = safeRowGet(index);
		
		try {
			RandomAccessFile randomAccessFile = new RandomAccessFile(rowFile, "rw");
			
			String[] columnsNames = rowFormat.keySet().toArray(new String[0]);
			HashMap<String, TableColumn> columns = new HashMap<String, TableColumn>();
			
			int offset = 0;
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
	
	public FileLock lockRow(int index) {
		
		File rowFile = safeRowGet(index);
		
		try {
			RandomAccessFile randomAccessFile = new RandomAccessFile(rowFile, "rw");
			
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
