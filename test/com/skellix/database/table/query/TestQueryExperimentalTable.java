package com.skellix.database.table.query;

import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.jupiter.api.Test;

import com.skellix.database.session.Session;
import com.skellix.database.table.ExperimentalTable;
import com.skellix.database.table.RowFormat;
import com.skellix.database.table.RowFormatter;
import com.skellix.database.table.RowFormatterException;
import com.skellix.database.table.TableFormatter;
import com.skellix.database.table.TableRow;
import com.skellix.database.table.query.exception.QueryParseException;
import com.skellix.database.table.query.node.QueryNode;

class TestQueryExperimentalTable {

	@Test
	void test() {
		
		{
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
			
			ExperimentalTable table = ExperimentalTable.getOrCreate(directory, rowFormat);
			try {
				table.deleteTable();
			} catch (IOException e) {
				e.printStackTrace();
				fail("Unable to delete table");
			}
			table.initTable();
			
			String insertQuery = "table 'testData/experimental' addRow {'name': '%s', 'age': %d}";
			
			try (Session session = Session.createNewSession(true)) {
				
				for (int i = 0 ; i < 100 ; i ++) {
					
					String formattedQuery = String.format(insertQuery, "test", (long) i);
					QueryNodeParser.parse(formattedQuery).query(session);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		String queryString = "(((table 'testData/experimental') as person where person.age <= 50) where (person.age <= 25) select 'name', 'age') limit 10";
		
		System.out.println("query string: " + queryString.toString());
		
		try (Session session = Session.createNewSession(false)) {
			
			Object result = QueryNodeParser.parse(queryString).query(session);
			
			if (result instanceof ExperimentalTable) {
				
				ExperimentalTable table = (ExperimentalTable) result;
				TableFormatter.printRows(table);
			}
			
		} catch (QueryParseException e) {
			e.printStackTrace();
			fail("Unable to parse query");
		} catch (Exception e1) {
			e1.printStackTrace();
		}
	}

}
