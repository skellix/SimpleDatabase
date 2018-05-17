package com.skellix.database.table.query;

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
import com.skellix.database.table.ExperimentalTable;
import com.skellix.database.table.TableFormat;
import com.skellix.database.table.TableFormatter;
import com.skellix.database.table.query.exception.QueryParseException;
import com.skellix.database.table.query.node.QueryNode;

class TestNodeQuery {

	@Test
	void test() {
		
		Path directory = Paths.get("testData", "experimental");
		
		StringBuilder sb = new StringBuilder();
		sb.append("{string name 32}");
		sb.append("{long age}");
		
		RowFormat rowFormat = null;
		try {
			rowFormat = RowFormatter.parse(sb.toString());
		} catch (RowFormatterException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		}
		
		Integer name = rowFormat.columnIndexes.get("name");
		Integer age = rowFormat.columnIndexes.get("age");
		
		ExperimentalTable table = ExperimentalTable.getOrCreate(directory, rowFormat);
		try {
			table.deleteTable();
		} catch (IOException e) {
			e.printStackTrace();
			fail("Unable to delete table");
		}
		table.initTable();
		
		try (Session session = Session.createNewSession(true)) {
			
			TableRow row = table.addRow(session);
			row.columns.get(name).set("test");
			row.columns.get(age).set(50L);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		QueryNode addRowQueryNode = null;
		try {
			String queryString = "table 'testData/experimental' addRow {'name' : 'test2', 'age' : 25}";
			addRowQueryNode = QueryNodeParser.parse(queryString);
		} catch (QueryParseException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		}
		
		try (Session session = Session.createNewSession(true)) {
			System.out.println("Starting query");
			Object result = session.query(addRowQueryNode);
			System.out.println("Query complete");
			
			TableFormatter.printTableStart(table, TableFormat.FORMAT_CELLS);
			table.stream().forEach(row -> TableFormatter.printTableRow(table, row, TableFormat.FORMAT_CELLS));
			TableFormatter.printTableEnd(table, TableFormat.FORMAT_CELLS);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
