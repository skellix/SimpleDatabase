package com.skellix.database.table;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.jupiter.api.Test;

import com.skellix.database.row.RowFormat;
import com.skellix.database.row.RowFormatter;
import com.skellix.database.row.RowFormatterException;
import com.skellix.database.row.TableRow;
import com.skellix.database.session.Session;

class TestExperimentalTable {

	@Test
	void test() {
		
		Path directory = Paths.get("testData", "experimental");
		
		if (Files.exists(directory)) {
			
			try {
				Files.walk(directory).forEach(path -> {
					
					System.out.println("deleting: " + path.toAbsolutePath().toString());
					
					if (!Files.isDirectory(path)) {
						
						try {
							Files.delete(path);
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				});
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		}
		
		StringBuilder sb = new StringBuilder();
		sb.append("{string username 16}");
		sb.append("{string password 32}");
		
		RowFormat rowFormat = null;
		try {
			rowFormat = RowFormatter.parse(sb.toString());
		} catch (RowFormatterException e1) {
			e1.printStackTrace();
		}
		
		Table table = Table.getOrCreate(directory, rowFormat);
		try {
			table.deleteTable();
		} catch (IOException e) {
			e.printStackTrace();
		}
		table.initTable();
		
		System.out.println("limit: " + table.buffer.limit());
		
		Integer username = rowFormat.columnIndexes.get("username");
		Integer password = rowFormat.columnIndexes.get("password");
		
		// 0       in    0.015s
		// 10      in    0.021s
		// 100     in    0.055s
		// 1000    in    0.295s
		// 10000   in    2.631s
		// 100000  in   25.587s
		// 1000000 in  264.326s
		
		try (Session session = Session.createNewSession(true)) {
			
			for (int i = 0 ; i < 1 ; i ++) {
				
				TableRow row = table.addRow(session);
				row.columns.get(username).set("test");
				row.columns.get(password).set("testpassword");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		System.out.println("limit: " + table.buffer.limit());
		System.out.println("row count: " + table.rowCount());
		
		System.out.println("running query");
		
		// 1       in    0.020s
		// 10      in    0.024s
		// 100     in    0.052s
		// 1000    in    0.310s
		// 10000   in    2.662s
		// 100000  in   26.541s
		// 1000000 in  263.457s
		
		long queryStart = System.currentTimeMillis();
		
		table.printAllRows(TableFormat.FORMAT_CELLS);
		
		long queryTime = System.currentTimeMillis() - queryStart;
		System.out.println("query time: " + queryTime + "ms");
		
		table.debugPrint();
		
	}

}
