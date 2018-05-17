package com.skellix.database.table;

import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.jupiter.api.Test;

import com.skellix.database.row.RowFormat;
import com.skellix.database.row.RowFormatter;
import com.skellix.database.row.RowFormatterException;
import com.skellix.database.row.TableRow;
import com.skellix.database.session.Session;

class TestTableMapSharedInstances {

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
		
		try (Session session = Session.createNewSession(true)) {
			
			for (int i = 0 ; i < 3 ; i ++) {
				
				TableRow row = table.addRow(session);
				row.columns.get(username).set("test");
				row.columns.get(password).set("testpassword");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		TableMap tableMap1 = TableMap.read(table, 0);
		TableMap tableMap2 = TableMap.read(table, 0);
		
		if (tableMap1 != tableMap2) {
			
			fail("The two TableMap instances were different");
		}
		
		table.debugPrint();
	}

}
