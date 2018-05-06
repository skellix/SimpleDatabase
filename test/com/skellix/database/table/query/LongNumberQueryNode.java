package com.skellix.database.table.query;

public class LongNumberQueryNode extends NumberQueryNode {
	
	private long value;

	public LongNumberQueryNode ofLong(long value) {
		
		this.value = value;
		return this;
	}

	@Override
	public Object query() throws Exception {
		
		return value;
	}
	
	@Override
	public String toString() {
		
		return Long.toString(value);
	}

}
