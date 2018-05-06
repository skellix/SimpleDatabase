package com.skellix.database.table.query;

public class ReturnVariableQueryNode extends QueryNode {

	public ReturnVariableQueryNode(VariableQueryNode returnNode) {
		
		copyValuesFrom(returnNode);
	}

	@Override
	public String generateCode() {
		
		StringBuilder sb = new StringBuilder();
		
		sb.append('{');
		sb.append(getLabel());
		sb.append(':');
		sb.append(getLabel());
		sb.append('}');
		
		return sb.toString();
	}

	@Override
	public Object query() throws Exception {
		
		QueryNode child = (QueryNode) children.get(0);
		return child.query();
	}

}
