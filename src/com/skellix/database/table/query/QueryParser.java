package com.skellix.database.table.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import treeparser.TreeNode;
import treeparser.TreeParser;

public class QueryParser {

	public static TableQuery parse(String queryString) throws QueryParseException {
		
		TreeNode queryRoot = TreeParser.parse(queryString);
		
		{
			List<TreeNode> list = descendantsOrSelfMatch(queryRoot, "^(?:\\-|\\+|)\\d+\\.\\d+$");
			
			if (list != null) {
				
				for (TreeNode match : list) {
					
					int index = match.getIndex();
					String label = match.getLabel();
					
					try {
					
						QueryNode queryNode = new DoubleNumberQueryNode(match);
						match.parent.children.remove(index);
						match.parent.children.add(index, queryNode);
						
					} catch (NumberFormatException e) {
						
						String errorString = String.format("ERROR: unable to parse number '%s' at %d, %d"
								, label, match.line, match.getStartColumn());
						
						throw new QueryParseException(errorString);
					}
				}
			}
		}
		
		{
			List<TreeNode> list = descendantsOrSelfMatch(queryRoot, "^(?:\\-|\\+|)\\d+$");
			
			if (list != null) {
				
				for (TreeNode match : list) {
					
					int index = match.parent.children.indexOf(match);
					String label = match.getLabel();
					
					try {
					
						QueryNode queryNode = new IntegerNumberQueryNode(match);
						match.parent.children.remove(index);
						match.parent.children.add(index, queryNode);
						
					} catch (NumberFormatException e) {
						
						String errorString = String.format("ERROR: unable to parse number '%s' at %d, %d"
								, label, match.line, match.getStartColumn());
						
						throw new QueryParseException(errorString);
					}
				}
			}
		}
		
		{
			List<TreeNode> list = descendantsOrSelfMatch(queryRoot, "^[a-zA-Z_]+$");
			
			if (list != null) {
				
				for (TreeNode match : list) {
					
					String label = match.getLabel();
					int index = match.getIndex();
					if (label.equals("if") || label.equals("else") || label.equals("return")) {
						
						continue;
					}
					
					try {
						QueryNode queryNode = new VariableQueryNode(match);
						match.parent.children.remove(index);
						match.parent.children.add(index, queryNode);
						
					} catch (VariableFormatException e) {
						
						String errorString = String.format("ERROR: unable to parse variable '%s' at %d, %d"
								, label, match.line, match.getStartColumn());
						
						throw new QueryParseException(errorString);
					}
				}
			}
		}
		
		{
			List<TreeNode> list = descendantsThatHaveChildren(queryRoot);
			
			if (list != null) {
				
				for (TreeNode match : list) {
					
					int index = match.getIndex();
					
					if (match.getEnterLabel().equals("(")) {
						
						QueryNode queryNode = new GroupQueryNode(match);
						match.parent.children.remove(index);
						match.parent.children.add(index, queryNode);
						queryNode.parent = match.parent;
						
						for (TreeNode child : match.children) {
							
							if (!child.getLabel().equals(",")) {
								
								queryNode.add(child);
							}
						}
					}
				}
			}
		}
		
		{
			List<TreeNode> list = descendantsOrSelfMatch(queryRoot, "^<=$");
			
			if (list != null) {
				
				for (TreeNode match : list) {
					
					LeftRightOperatorQueryNode.parse(match, LessThanOrEqualsQueryNode.class);
				}
			}
		}
		
		Set<String> columnReslts = new LinkedHashSet<String>();
		
		{
			List<TreeNode> list = descendantsOrSelfMatch(queryRoot, "^return$");
			
			if (list != null) {
				
				for (int i = 0 ; i < list.size() ; i ++) {
					
					TreeNode match = list.get(i);
					
					QueryNode queryNode = new ReturnQueryNode(match);
					TreeNode returnNode = match.getNextSibling();
					
					if (returnNode == null) {
						
						String errorString = String.format("ERROR: Expected operand after '%s' at %d, %d"
								, match.getLabel(), match.line, match.getStartColumn());
						
						throw new QueryParseException(errorString);
					}
					int index = match.getIndex();
					match.parent.children.remove(index);
					match.parent.children.add(index, queryNode);
					returnNode.parent.children.remove(returnNode);
					queryNode.add(returnNode);
					
					if (returnNode instanceof GroupQueryNode) {
						
						GroupQueryNode groupNode = (GroupQueryNode) returnNode;
						
						for (int j = 0 ; j < groupNode.children.size() ; j ++) {
							
							TreeNode child = groupNode.children.get(j);
							
							if (child instanceof VariableQueryNode) {
								
								VariableQueryNode variableQueryNode = (VariableQueryNode) child;
								int index2 = variableQueryNode.getIndex();
								groupNode.children.remove(variableQueryNode);
								QueryNode child2 = new ReturnVariableQueryNode(variableQueryNode);
								groupNode.children.add(index2, child2);
								columnReslts.add(variableQueryNode.getLabel());
							}
						}
						
					} else if (returnNode instanceof VariableQueryNode) {
						
						VariableQueryNode variableQueryNode = (VariableQueryNode) returnNode;
						
						int index2 = variableQueryNode.getIndex();
						returnNode.children.remove(variableQueryNode);
						QueryNode child = new ReturnVariableQueryNode(variableQueryNode);
						returnNode.children.add(index2, child);
						columnReslts.add(variableQueryNode.getLabel());
					}
				}
			}
		}
		
		{
			List<TreeNode> list = descendantsOrSelfMatch(queryRoot, "^if$");
			
			if (list != null) {
				
				for (TreeNode match : list) {
					
					QueryNode queryNode = new IfQueryNode();
					queryNode.copyValuesFrom(match);
					
					TreeNode checkNode = match.getNextSibling();
					
					if (checkNode == null) {
						
						String errorString = String.format("ERROR: Expected check after '%s' at %d, %d"
								, match.getLabel(), match.line, match.getStartColumn());
						
						throw new QueryParseException(errorString);
					}
					checkNode.parent.children.remove(checkNode);
					queryNode.add(checkNode);
					
					TreeNode thenNode = match.getNextSibling();
					
					if (thenNode == null) {
						
						String errorString = String.format("ERROR: Expected operand after check of '%s' at %d, %d"
								, match.getLabel(), match.line, match.getStartColumn());
						
						throw new QueryParseException(errorString);
					}
					thenNode.parent.children.remove(thenNode);
					queryNode.add(thenNode);
					
					int index = match.getIndex();
					match.parent.children.remove(index);
					match.parent.children.add(index, queryNode);
				}
			}
		}
		
		if (!queryRoot.hasChildren()) {
			
			String errorString = String.format("ERROR: didn't find any operators in query after processing at %d, %d"
					, queryRoot.line, queryRoot.getStartColumn());
			
			throw new QueryParseException(errorString);
		}
		
		return new TableQuery((QueryNode) queryRoot.getFirstChild(), columnReslts);
	}
	
	private static List<TreeNode> descendantsThatHaveChildren(TreeNode treeNode) {
		
		if (treeNode.hasChildren()) {
			
			List<TreeNode> results = new ArrayList<TreeNode>();
			
			for (TreeNode child : treeNode.children) {
				
				List<TreeNode> childMatches = descendantsOrSelfThatHaveChildren(child);
				
				if (childMatches != null) {
				
					results.addAll(childMatches);
				}
			}
			
			return results;
		}
		
		return null;
	}
	
	private static List<TreeNode> descendantsOrSelfThatHaveChildren(TreeNode treeNode) {
		
		if (treeNode.hasChildren()) {
			
			List<TreeNode> results = new ArrayList<TreeNode>();
			results.add(treeNode);
			
			for (TreeNode child : treeNode.children) {
				
				List<TreeNode> childMatches = descendantsThatHaveChildren(child);
				
				if (childMatches != null) {
				
					results.addAll(childMatches);
				}
			}
			
			return results;
		}
		
		return null;
	}

	public static List<TreeNode> descendantsOrSelfMatch(TreeNode treeNode, String regex) {
		
		if (treeNode.hasChildren()) {
			
			List<TreeNode> results = new ArrayList<TreeNode>();
			
			for (TreeNode child : treeNode.children) {
				
				List<TreeNode> childMatches = descendantsOrSelfMatch(child, regex);
				
				if (childMatches != null) {
				
					results.addAll(childMatches);
				}
			}
			
			return results;
		}
		
		if (treeNode.getLabel().matches(regex)) {
			
			return Arrays.asList(treeNode);
		}
		
		return null;
	}

}
