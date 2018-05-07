package com.skellix.database.table.query;

import com.skellix.database.session.Session;

public class LongNumberQueryNode extends NumberQueryNode {
	
	private long value;

	public LongNumberQueryNode ofLong(long value) {
		
		this.value = value;
		return this;
	}

	@Override
	public Object query(Session session) throws Exception {
		
		return value;
	}
	
	@Override
	public String toString() {
		
		return Long.toString(value);
	}

}
