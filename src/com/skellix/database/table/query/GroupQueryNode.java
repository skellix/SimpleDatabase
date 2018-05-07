package com.skellix.database.table.query;

import java.util.ArrayList;
import java.util.List;

import com.skellix.database.session.Session;

import treeparser.TreeNode;

public class GroupQueryNode extends QueryNode {
	
	public static GroupQueryNode parse(TreeNode replaceNode) throws QueryParseException {
		
		int index = replaceNode.getIndex();
		
		GroupQueryNode queryNode = new GroupQueryNode(replaceNode);
		replaceNode.parent.children.remove(index);
		replaceNode.parent.children.add(index, queryNode);
		queryNode.parent = replaceNode.parent;
		
		for (TreeNode child : replaceNode.children) {
			
			if (!child.getLabel().equals(",")) {
				
				queryNode.add(child);
			}
		}
		
		return queryNode;
	}

	public GroupQueryNode(TreeNode from) {
		
		copyValuesFrom(from);
	}

	@Override
	public String generateCode() {
		
		StringBuilder sb = new StringBuilder();
		
		sb.append('[');
		
		TreeNode last = null;
		for (TreeNode childNode : children) {
			if (last != null) {
				
				sb.append(", ");
			}
			last = childNode;
			if (childNode instanceof QueryNode) {
				
				QueryNode child = (QueryNode) childNode;
				sb.append(child.generateCode());
			}
		}
		
		sb.append(']');
		
		return sb.toString();
	}
	
	@Override
	public String toString() {
		
		return generateCode();
	}

	@Override
	public Object query(Session session) throws Exception {
		
		List<QueryNode> list = new ArrayList<>();
		
		for (TreeNode childNode : children) {
			
			if (childNode instanceof QueryNode) {
				
				QueryNode child = (QueryNode) childNode;
				list.add(child);
			}
		}
		
		return list;
	}

}
