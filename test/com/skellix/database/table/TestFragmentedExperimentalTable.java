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
		
		Map<String, Integer> tableFormat = new LinkedHashMap<>();
		tableFormat.put("username", 16);
		tableFormat.put("password", 32);
		
		ExperimentalTable table = new ExperimentalTable(directory, tableFormat);
		try {
			table.deleteTable();
			table.initTable();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		System.out.println("limit: " + table.buffer.limit());
		
		Integer USERNAME = table.columnOffset.get("username");
		Integer PASSWORD = table.columnOffset.get("password");
		
		// 1       in   0.058s
		// 10      in   0.074s
		// 100     in   0.132s
		// 1000    in   0.514s
		// 10000   in   1.982s
		// 100000  in  12.089s
		// 1000000 in 114.345s
		
		for (int i = 0 ; i < 10000 ; i ++) {
			
			TableRow row0 = table.addRow();
			row0.setString(USERNAME, "test");
			row0.setString(PASSWORD, "testpassword");
			
			TableRow row1 = table.addRow();
			row1.setString(USERNAME, "test");
			row1.setString(PASSWORD, "testpassword");
			
			TableRow row2 = table.addRow();
			row2.setString(USERNAME, "test");
			row2.setString(PASSWORD, "testpassword");
			
			table.deleteRow(row1);
//			System.out.println("i:" + i);
			table.clean();
//			table.debugPrint();
		}
		
		System.out.println("limit: " + table.buffer.limit());
		System.out.println("row count: " + table.rowCount());
		
//		table.debugPrint();
//		table.clean();
//		table.debugPrint();
	}

}
