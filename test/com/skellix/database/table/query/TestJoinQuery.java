package com.skellix.database.table.query;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.jupiter.api.Test;

import com.skellix.database.row.RowFormatter;
import com.skellix.database.row.RowFormatterException;
import com.skellix.database.session.Session;
import com.skellix.database.table.Table;
import com.skellix.database.table.TableFormatter;

class TestJoinQuery {

	@Test
	void test() {
		
		Path directory1 = Paths.get("testData", "names");
		Path directory2 = Paths.get("testData", "ages");
		
		String table1Format = "{int key}{string name 32}";
		String table2Format = "{int key}{long age}";
		
		try {
			
			Table table1 = Table.getOrCreate(directory1, RowFormatter.parse(table1Format));
			Table table2 = Table.getOrCreate(directory2, RowFormatter.parse(table2Format));
			
			table1.deleteTable();
			table2.deleteTable();
			
			table1.initTable();
			table2.initTable();
			
			try (Session session = Session.createNewSession(true)) {
				QueryNodeParser.parse("table 'testData/names' addRow {'key' : 0, 'name' : 'john'}").query(session);
				QueryNodeParser.parse("table 'testData/ages' addRow {'key' : 0, 'age' : 50}").query(session);
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			try (Session session = Session.createNewSession(false)) {
				Object result = QueryNodeParser.parse("table 'testData/names' join table 'testData/ages' on names.key == ages.key").query(session);
				
				if (result instanceof Table) {
					
					Table table = (Table) result;
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
