package com.skellix.database.table.query;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.jupiter.api.Test;

import com.skellix.database.row.RowFormatter;
import com.skellix.database.row.RowFormatterException;
import com.skellix.database.session.Session;
import com.skellix.database.table.ExperimentalTable;
import com.skellix.database.table.TableFormatter;

class TestMultiJoinQuery {

	@Test
	void test() {
		
		Path testDir = Paths.get("testData");
		
		Path directory1 = testDir.resolve("names");
		Path directory2 = testDir.resolve("ages");
		Path directory3 = testDir.resolve("height");
		
		String table1Format = "{int key}{string name 32}";
		String table2Format = "{int key}{long age}";
		String table3Format = "{int key}{double height}";
		
		try {
			
			ExperimentalTable table1 = ExperimentalTable.getOrCreate(directory1, RowFormatter.parse(table1Format));
			ExperimentalTable table2 = ExperimentalTable.getOrCreate(directory2, RowFormatter.parse(table2Format));
			ExperimentalTable table3 = ExperimentalTable.getOrCreate(directory3, RowFormatter.parse(table3Format));
			
			table1.deleteTable();
			table2.deleteTable();
			table3.deleteTable();
			
			table1.initTable();
			table2.initTable();
			table3.initTable();
			
			try (Session session = Session.createNewSession(true)) {
				QueryNodeParser.parse("table 'testData/names' addRow {'key' : 0, 'name' : 'john'}").query(session);
				QueryNodeParser.parse("table 'testData/ages' addRow {'key' : 0, 'age' : 50}").query(session);
				QueryNodeParser.parse("table 'testData/height' addRow {'key' : 0, 'height' : 5.2}").query(session);
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			try (Session session = Session.createNewSession(false)) {
				
				StringBuilder sb = new StringBuilder();
				sb.append("table 'testData/names'");
				sb.append(" join table 'testData/ages' on names.key == ages.key");
				sb.append(" join table 'testData/height' on names_ages.key == height.key");
				sb.append(" getRows");
				
				Object result = QueryNodeParser.parse(sb.toString()).query(session);
				
				if (result instanceof ExperimentalTable) {
					
					ExperimentalTable table = (ExperimentalTable) result;
					TableFormatter.printRows(table);
				}
				
			} catch (Exception e) {
				e.printStackTrace();
			}
			
		} catch (RowFormatterException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
