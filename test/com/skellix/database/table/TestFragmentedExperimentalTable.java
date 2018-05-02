package com.skellix.database.table;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

class TestFragmentedExperimentalTable {

	@Test
	void test() {
		
		Path directory = Paths.get("testData", "experimental");
		
		StringBuilder sb = new StringBuilder();
		sb.append("{string username 16}");
		sb.append("{string password 32}");
		
		RowFormat rowFormat = RowFormatter.parse(sb.toString());
		
		Integer username = rowFormat.columnIndexes.get("username");
		Integer password = rowFormat.columnIndexes.get("password");
		
		ExperimentalTable table = new ExperimentalTable(directory, rowFormat);
		try {
			table.deleteTable();
			table.initTable();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		System.out.println("limit: " + table.buffer.limit());
		
		// 1       in   0.058s
		// 10      in   0.074s
		// 100     in   0.132s
		// 1000    in   0.514s
		// 10000   in   1.982s
		// 100000  in  12.089s
		// 1000000 in 114.345s
		
		for (int i = 0 ; i < 10000 ; i ++) {
			
			TableRow row0 = table.addRow();
			row0.columns.get(username).set("test");
			row0.columns.get(password).set("testpassword");
			
			TableRow row1 = table.addRow();
			row1.columns.get(username).set("test");
			row1.columns.get(password).set("testpassword");
			
			TableRow row2 = table.addRow();
			row2.columns.get(username).set("test");
			row2.columns.get(password).set("testpassword");
			
			table.deleteRow(row1);
		}
		
		System.out.println("limit: " + table.buffer.limit());
		System.out.println("row count: " + table.rowCount());
		
		table.debugPrint();
		table.clean();
		table.debugPrint();
	}

}
