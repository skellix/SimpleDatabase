package com.skellix.database.table.query;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Stream;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import com.skellix.database.row.RowFormat;
import com.skellix.database.table.Table;
import com.skellix.database.table.query.node.QueryNode;

public class TableQuery {

	private QueryNode queryNode;
	private Set<String> columnLabels;

	public TableQuery(QueryNode queryNode, Set<String> columnReslts) {
		
		this.queryNode = queryNode;
		this.columnLabels = columnReslts;
	}

	public TableQueryResults queryTable(Table table) {
		
		ScriptEngineManager scriptEngineManager = new ScriptEngineManager();
		ScriptEngine scriptEngine = scriptEngineManager.getEngineByName("js");
		
		RowFormat rowFormat = table.rowFormat;
		List<String> columnNames = table.rowFormat.columnNames;
		Map<String, Integer> columnIndexes = rowFormat.columnIndexes;
		Set<Entry<String, Integer>> columns = columnIndexes.entrySet();
		
		
		StringBuilder sb = new StringBuilder();
		sb.append("var rowFunc = function(");
		String last = null;
		for (String columnName : columnNames) {
			
			if (last != null) {
				
				sb.append(',');
			}
			last = columnName;
			sb.append(columnName);
		}
		sb.append(") {");
		sb.append(queryNode.generateCode());
		sb.append("};");
		
//		System.out.println(sb.toString());
		
		try {
			scriptEngine.eval(sb.toString());
		} catch (ScriptException e1) {
			e1.printStackTrace();
		}
		
		Invocable invocable = (Invocable) scriptEngine;
		
		Stream<Object> results = table.stream()
			.map(row -> {
			
				try {
					Object[] values = row.getValues();
					Object o = invocable.invokeFunction("rowFunc", values);
					return o;
				} catch (NoSuchMethodException e) {
					e.printStackTrace();
				} catch (ScriptException e) {
					e.printStackTrace();
				}
				
				return null;
			})
			.filter(result -> result != null);
		
		return new TableQueryResults(results, columnLabels);
	}

}
