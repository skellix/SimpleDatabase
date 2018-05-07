package com.skellix.database.table.query;

import java.util.ArrayList;
import java.util.List;

import com.skellix.database.session.Session;

import treeparser.TreeNode;

public class MapQueryNode extends QueryNode {
	
	private List<EntryQueryNode> map;
	
	public static MapQueryNode parse(TreeNode replaceNode) throws QueryParseException {
		
		List<EntryQueryNode> map = new ArrayList<>();
		
		for (int i = 0 ; i < replaceNode.children.size() ; i ++) {
			
			TreeNode child = replaceNode.children.get(i);
			
			if (child instanceof EntryQueryNode) {
				
				EntryQueryNode entry = (EntryQueryNode) child;
				map.add(entry);
				
			} else {
				
				if (child.hasChildren() || !child.getLabel().equals(",")) {
					
					String errorString = String.format("ERROR: found invalid token '%s' in map at %d, %d"
							, child.getLabel(), replaceNode.line, replaceNode.getStartColumn());
					
					throw new QueryParseException(errorString);
				}
			}
		}
		
		MapQueryNode queryNode = new MapQueryNode().ofMap(map);
		queryNode.parent = replaceNode.parent;
		
		int index = replaceNode.getIndex();
		replaceNode.parent.children.remove(replaceNode);
		replaceNode.parent.children.add(index, queryNode);
		
		return queryNode;
	}

	public MapQueryNode ofMap(List<EntryQueryNode> map) {
		
		this.map = map;
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
