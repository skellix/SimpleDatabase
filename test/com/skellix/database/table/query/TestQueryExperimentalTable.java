package com.skellix.database.table.query;

import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.jupiter.api.Test;

import com.skellix.database.table.ExperimentalTable;
import com.skellix.database.table.RowFormat;
import com.skellix.database.table.RowFormatter;
import com.skellix.database.table.TableFormat;
import com.skellix.database.table.TableFormatter;
import com.skellix.database.table.TableRow;

class TestQueryExperimentalTable {

	@Test
	void test() {
		
		Path directory = Paths.get("testData", "experimental");
		
		StringBuilder sb = new StringBuilder();
		sb.append("{string name 32}");
		sb.append("{long age}");
		
		RowFormat rowFormat = RowFormatter.parse(sb.toString());
		
		Integer name = rowFormat.columnIndexes.get("name");
		Integer age = rowFormat.columnIndexes.get("age");
		
		ExperimentalTable table = new ExperimentalTable(directory, rowFormat);
		try {
			table.deleteTable();
		} catch (IOException e) {
			e.printStackTrace();
			fail("Unable to delete table");
		}
		table.initTable();
		
		for (int i = 0 ; i < 100 ; i ++) {
			
			TableRow row = table.addRow();
			row.columns.get(name).set("test");
			row.columns.get(age).set((long) i);
		}
		
		StringBuilder queryString = new StringBuilder();
		queryString.append("if age <= 50 return (name, age)");
		
		System.out.println("query string: " + queryString.toString());
		
		try {
			TableQuery query = QueryParser.parse(queryString.toString());
			TableQueryResults result = query.queryTable(table);
			
			TableFormatter.printTableStart(table, TableFormat.FORMAT_CELLS);
			
			result.printResults(table, TableFormat.FORMAT_CELLS);
			
			TableFormatter.printTableEnd(table, TableFormat.FORMAT_CELLS);
			
		} catch (QueryParseException e) {
			e.printStackTrace();
			fail("Unable to parse query");
		}
	}

}
