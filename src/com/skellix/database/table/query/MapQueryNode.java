package com.skellix.database.table.query;

import java.util.List;

public class MapQueryNode extends QueryNode {
	
	private List<EntryQueryNode> map;

	public MapQueryNode ofMap(List<EntryQueryNode> map) {
		
		this.map = map;
		return this;
	}

	@Override
	public Object query() throws Exception {
		
		return map;
	}

	@Override
	public String generateCode() {
		
		StringBuilder sb = new StringBuilder();
		sb.append('{');
		EntryQueryNode last = null;
		
		for (EntryQueryNode entry : map) {
			
			if (last != null) {
				
				sb.append(", ");
			}
			last = entry;
			sb.append(entry.generateCode());
		}
		
		sb.append('}');
		return sb.toString();
	}
	
	@Override
	public String toString() {
		
		return generateCode();
	}

}
