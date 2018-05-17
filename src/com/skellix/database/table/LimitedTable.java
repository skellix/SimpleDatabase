package com.skellix.database.table;

import java.util.stream.Stream;

import com.skellix.database.row.RowFormat;
import com.skellix.database.row.TableRow;
import com.skellix.database.session.Session;

public class LimitedTable extends ExperimentalTable {
	
	ExperimentalTable source = null;
	private long limit;
	
	public LimitedTable() {
		//
	}

	public LimitedTable limit(RowFormat rowFormat, long limit) {
		
		this.rowFormat = rowFormat;
		this.limit = limit;
		return this;
	}

	public void setSource(String name, ExperimentalTable table) {
		
		source = table;
	}

	@Override
	public String getName() {
		
		return source.getName();
	}
	
	@Override
	public TableRow addRow(Session session) {
		
		return new CombinedTableRow().map(rowFormat);
	}
	
	@Override
	public Stream<TableRow> stream() {
		
		return source.stream().limit(limit);
	}

}
