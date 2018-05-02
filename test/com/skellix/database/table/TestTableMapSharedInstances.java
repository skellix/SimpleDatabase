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
		
		for (int i = 0 ; i < 3 ; i ++) {
			
			TableRow row = table.addRow();
			row.columns.get(username).set("test");
			row.columns.get(password).set("testpassword");
		}
		
		TableMap tableMap1 = TableMap.read(table, 0);
		TableMap tableMap2 = TableMap.read(table, 0);
		
		if (tableMap1 != tableMap2) {
			
			fail("The two TableMap instances were different");
		}
		
		table.debugPrint();
	}

}
