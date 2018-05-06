package com.skellix.database.table.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import treeparser.TreeNode;
import treeparser.TreeParser;

public class QueryNodeParser {
	
	public static QueryNode parse(String source) throws QueryParseException {
		
		TreeNode tree = TreeParser.parse(source);
		
		parseTree(tree);
		
		TreeNode firstChild = tree.getFirstChild();
		
		if (firstChild instanceof QueryNode) {
			
			return (QueryNode) firstChild;
		}
		return null;
	}

	private static void parseTree(TreeNode tree) throws QueryParseException {
		
		parseSingleTokensRule.parse(tree);
		parseGroupRule.parse(tree);
		parseSimpleRightTokensRule.parse(tree);
		parseSimpleLeftRightTokensRule.parse(tree);
		parseComplexTokensRule.parse(tree);
	}
	
	private static QueryParserRule parseFloatingPointNumbersRule = (tree) -> {
		
		List<TreeNode> list = descendantsOrSelfMatch(tree, "^(?:\\-|\\+|)\\d+\\.\\d+$");
		
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
	};
	
	private static QueryParserRule parseIntegerNumbersRule = (tree) -> {
		
		List<TreeNode> list = descendantsOrSelfMatch(tree, "^(?:\\-|\\+|)\\d+$");
		
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
	};
	
	private static QueryParserRule parseNumbersRule = (tree) -> {
		
		parseFloatingPointNumbersRule.parse(tree);
		parseIntegerNumbersRule.parse(tree);
	};
	
	private static QueryParserRule parseStringsRule = (tree) -> {
		
		List<TreeNode> list = descendantsOrSelfMatch(tree, "^(['\"]).+\\1$");
		
		if (list != null) {
			
			for (TreeNode match : list) {
				
				int index = match.parent.children.indexOf(match);
				String label = match.getLabel();
				
				String withoutQuotes = label.substring(1, label.length() - 1);
				
				StringQueryNode queryNode = new StringQueryNode().ofString(withoutQuotes);
				queryNode.parent = match.parent;
				
				match.parent.children.remove(index);
				match.parent.children.add(index, queryNode);
			}
		}
	};
	
	private static QueryParserRule parseSingleTokensRule = (tree) -> {
		
		parseNumbersRule.parse(tree);
		parseStringsRule.parse(tree);
	};
	
	private static QueryParserRule parseEntriesRule = (tree) -> {
		
		List<TreeNode> list = descendantsOrSelfMatch(tree, "^:$");
		
		if (list != null) {
			
			for (TreeNode match : list) {
				
				EntryQueryNode.parse(match);
			}
		}
	};
	
	private static QueryParserRule parseSimpleLeftRightTokensRule = (tree) -> {
		
		parseEntriesRule.parse(tree);
	};
	
	private static QueryParserRule parseTableTokensRule = (tree) -> {
		
		List<TreeNode> list = descendantsOrSelfMatch(tree, "^table$");
		
		if (list != null) {
			
			for (TreeNode match : list) {
				
				int index = match.parent.children.indexOf(match);
				String label = match.getLabel();
				
				TreeNode nextNode = match.getNextSibling();
				
				if (nextNode == null || !(nextNode instanceof StringQueryNode)) {
					
					String errorString = String.format("ERROR: expected string after '%s' in %d, %d"
							, match.getLabel(), match.line, match.getStartColumn());
					
					throw new QueryParseException(errorString);
				}
				
				TableQueryNode queryNode = new TableQueryNode().getTable((QueryNode) nextNode);
				queryNode.parent = match.parent;
				
				match.parent.children.remove(index);
				match.parent.children.add(index, queryNode);
				
				nextNode.parent.children.remove(nextNode);
			}
		}
	};
	
	private static QueryParserRule parseSimpleRightTokensRule = (tree) -> {
		
		parseTableTokensRule.parse(tree);
	};
	
	private static QueryParserRule parseGroupRule = (tree) -> {
		
		List<TreeNode> list = descendantsThatHaveChildren(tree);
		
		if (list != null) {
			
			for (TreeNode match : list) {
				
				if (match.getEnterLabel().equals("(")) {
					
					int index = match.getIndex();
					
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
	};
	
	private static QueryParserRule parseMapRule = (tree) -> {
		
		List<TreeNode> list = descendantsThatHaveChildren(tree);
		
		if (list != null) {
			
			for (TreeNode match : list) {
				
				if (match.getEnterLabel().equals("{")) {
					
					List<EntryQueryNode> map = new ArrayList<>();
					
					for (int i = 0 ; i < match.children.size() ; i ++) {
						
						TreeNode child = match.children.get(i);
						
						if (child instanceof EntryQueryNode) {
							
							EntryQueryNode entry = (EntryQueryNode) child;
							map.add(entry);
							
						} else {
							
							if (child.hasChildren() || !child.getLabel().equals(",")) {
								
								String errorString = String.format("ERROR: found invalid token '%s' in map at %d, %d"
										, child.getLabel(), match.line, match.getStartColumn());
								
								throw new QueryParseException(errorString);
							}
						}
					}
					
					MapQueryNode queryNode = new MapQueryNode().ofMap(map);
					queryNode.parent = match.parent;
					
					int index = match.getIndex();
					match.parent.children.remove(match);
					match.parent.children.add(queryNode);
				}
			}
		}
	};
	
	private static QueryParserRule parseAddRowRule = (tree) -> {
		
		List<TreeNode> list = descendantsOrSelfMatch(tree, "^addRow$");
		
		if (list != null) {
			
			for (TreeNode match : list) {
				
				AddRowQueryNode.parse(match);
			}
		}
	};
	
	private static QueryParserRule parseComplexTokensRule = (tree) -> {
		
		parseMapRule.parse(tree);
		parseAddRowRule.parse(tree);
	};
	
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
			
		} else {
			
			String label = treeNode.getLabel();
			
			if (label != null && label.matches(regex)) {
				
				return Arrays.asList(treeNode);
			}
		}
		
		return null;
	}

}
