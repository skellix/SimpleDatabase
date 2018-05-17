package com.skellix.database.table;

import java.util.stream.Stream;

import com.skellix.database.session.Session;
import com.skellix.database.table.query.node.QueryNode;

public class AliasedTable extends ExperimentalTable {
	
	ExperimentalTable source = null;
	private String alias;
	
	public AliasedTable() {
		//
	}

	public AliasedTable filter(RowFormat rowFormat) {
		
		this.rowFormat = rowFormat;
		return this;
	}

	public void setSource(String name, ExperimentalTable table) {
		
		source = table;
	}

	@Override
	public String getName() {
		
		return alias;
	}
	
	@Override
	public TableRow addRow(Session session) {
		
		return new CombinedTableRow().map(rowFormat);
	}
	
	@Override
	public Stream<TableRow> stream() {
		
		return source.stream();
	}

	public void setAlias(String alias) {
		
		this.alias = alias;
	}

}
