package com.skellix.database.table;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

class TestTableMapSharedInstances {

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
		
		Integer USERNAME = table.columnOffset.get("username");
		Integer PASSWORD = table.columnOffset.get("password");
		
		for (int i = 0 ; i < 3 ; i ++) {
			
			TableRow row = table.addRow();
			row.setString(USERNAME, "test");
			row.setString(PASSWORD, "testpassword");
		}
		
		TableMap tableMap1 = TableMap.read(table, 0);
		TableMap tableMap2 = TableMap.read(table, 0);
		
		if (tableMap1 != tableMap2) {
			
			fail("The two TableMap instances were different");
		}
		
		table.debugPrint();
	}

}
