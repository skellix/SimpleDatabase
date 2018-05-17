package com.skellix.database.table;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.jupiter.api.Test;

import com.skellix.database.row.RowFormat;
import com.skellix.database.row.RowFormatter;
import com.skellix.database.row.RowFormatterException;
import com.skellix.database.row.TableRow;
import com.skellix.database.session.Session;

class TestFragmentedExperimentalTable {

	@Test
	void test() {
		
		Path directory = Paths.get("testData", "experimental");
		
		StringBuilder sb = new StringBuilder();
		sb.append("{string username 16}");
		sb.append("{string password 32}");
		
		RowFormat rowFormat = null;
		try {
			rowFormat = RowFormatter.parse(sb.toString());
		} catch (RowFormatterException e1) {
			e1.printStackTrace();
		}
		
		Integer username = rowFormat.columnIndexes.get("username");
		Integer password = rowFormat.columnIndexes.get("password");
		
		Table table = Table.getOrCreate(directory, rowFormat);
		try {
			table.deleteTable();
			table.initTable();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
//		System.out.println("limit: " + table.buffer.limit());
		
		// 1       in   0.030s
		// 10      in   0.034s
		// 100     in   0.086s
		// 1000    in   0.423s
		// 10000   in   3.326s
		// 100000  in  33.035s
		// 1000000 in 114.345s
		
		try (Session session = Session.createNewSession(true)) {
		
			for (int i = 0 ; i < 100 ; i ++) {
				
				TableRow row0 = table.addRow(session);
				row0.columns.get(username).set("test");
				row0.columns.get(password).set("testpassword");
				
				TableRow row1 = table.addRow(session);
				row1.columns.get(username).set("test");
				row1.columns.get(password).set("testpassword");
				
				TableRow row2 = table.addRow(session);
				row2.columns.get(username).set("test");
				row2.columns.get(password).set("testpassword");
				
				table.deleteRow(row1, session);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
//		System.out.println("limit: " + table.buffer.limit());
//		System.out.println("row count: " + table.rowCount());
//		
//		table.debugPrint();
//		table.clean();
//		table.debugPrint();
	}

}
