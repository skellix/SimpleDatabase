package com.skellix.database.table.query.node;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import com.skellix.database.session.Session;
import com.skellix.database.table.query.exception.QueryParseException;

import treeparser.TreeNode;

public class MapQueryNode extends QueryNode {
	
	private List<QueryNode> map;
	
	public MapQueryNode parse(TreeNode replaceNode) throws QueryParseException {
		
		List<QueryNode> map = new ArrayList<>();
		
		for (int i = 0 ; i < replaceNode.children.size() ; i ++) {
			
			TreeNode child = replaceNode.children.get(i);
			
			if (child instanceof QueryNode) {
				
				QueryNode entry = (QueryNode) child;
				
				if (!Entry.class.isAssignableFrom(entry.resultType)) {
					
					String errorString = String.format("ERROR: Token '%s' in map is not an entry at %d, %d"
							, child.getLabel(), child.line, child.getStartColumn());
					
					throw new QueryParseException(errorString);
				}
				
				map.add(entry);
				
			} else {
				
				if (child.hasChildren() || !child.getLabel().equals(",")) {
					
					String errorString = String.format("ERROR: found invalid token '%s' in map at %d, %d"
							, child.getLabel(), replaceNode.line, replaceNode.getStartColumn());
					
					throw new QueryParseException(errorString);
				}
			}
		}
		
		ofMap(map);
		parent = replaceNode.parent;
		replaceNode.replaceWith(this);
		resultType = List.class;
		
		return this;
	}

	public MapQueryNode ofMap(List<QueryNode> map) {
		
		this.map = map;
		
		this.children.clear();
		
		for (QueryNode entry : map) {
		
			this.children.add(entry);
		}
		
		return this;
	}

	@Override
	public Object query(Session session) throws Exception {
		
		return map;
	}

	@Override
	public String generateCode() {
		
		StringBuilder sb = new StringBuilder();
		sb.append('{');
		QueryNode last = null;
		
		for (QueryNode entry : map) {
			
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
