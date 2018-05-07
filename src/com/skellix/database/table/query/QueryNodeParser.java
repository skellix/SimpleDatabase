package com.skellix.database.table.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

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
		
		forAllDescendantsOrSelfThatMatch(tree, "^(?:\\-|\\+|)\\d+\\.\\d+$",
				match -> DoubleNumberQueryNode.parse(match));
	};
	
	private static QueryParserRule parseIntegerNumbersRule = (tree) -> {
		
		forAllDescendantsOrSelfThatMatch(tree, "^(?:\\-|\\+|)\\d+$",
				match -> IntegerNumberQueryNode.parse(match));
	};
	
	private static QueryParserRule parseNumbersRule = (tree) -> {
		
		parseFloatingPointNumbersRule.parse(tree);
		parseIntegerNumbersRule.parse(tree);
	};
	
	private static QueryParserRule parseStringsRule = (tree) -> {
		
		forAllDescendantsOrSelfThatMatch(tree, "^(['\"]).+\\1$",
				match -> StringQueryNode.parse(match));
	};
	
	private static QueryParserRule parseSingleTokensRule = (tree) -> {
		
		parseNumbersRule.parse(tree);
		parseStringsRule.parse(tree);
	};
	
	private static QueryParserRule parseEntriesRule = (tree) -> {
		
		forAllDescendantsOrSelfThatMatch(tree, "^:$",
				match -> EntryQueryNode.parse(match));
	};
	
	private static QueryParserRule parseSimpleLeftRightTokensRule = (tree) -> {
		
		parseEntriesRule.parse(tree);
	};
	
	private static QueryParserRule parseTableTokensRule = (tree) -> {
		
		forAllDescendantsOrSelfThatMatch(tree, "^table$",
				match -> TableQueryNode.parse(match));
	};
	
	private static QueryParserRule parseSimpleRightTokensRule = (tree) -> {
		
		parseTableTokensRule.parse(tree);
	};
	
	private static QueryParserRule parseGroupRule = (tree) -> {
		
		List<TreeNode> list = descendantsThatHaveChildren(tree);
		
		if (list != null) {
			
			for (TreeNode match : list) {
				
				if (match.getEnterLabel().equals("(")) {
					
					GroupQueryNode.parse(match);
				}
			}
		}
	};
	
	private static QueryParserRule parseMapRule = (tree) -> {
		
		List<TreeNode> list = descendantsThatHaveChildren(tree);
		
		if (list != null) {
			
			for (TreeNode match : list) {
				
				if (match.getEnterLabel().equals("{")) {
					
					MapQueryNode.parse(match);
				}
			}
		}
	};
	
	private static QueryParserRule parseAddRowRule = (tree) -> {
		
		forAllDescendantsOrSelfThatMatch(tree, "^addRow$",
				match -> AddRowQueryNode.parse(match));
	};
	
	private static QueryParserRule parseGetRowsRule = (tree) -> {
		
		forAllDescendantsOrSelfThatMatch(tree, "^getRows$",
				match -> GetRowsQueryNode.parse(match));
	};
	
	private static QueryParserRule parseComplexTokensRule = (tree) -> {
		
		parseMapRule.parse(tree);
		parseAddRowRule.parse(tree);
		parseGetRowsRule.parse(tree);
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
		
		return Arrays.asList();
	}
	
	public static void forAllDescendantsOrSelfThatMatch(TreeNode treeNode, String regex, QueryParserRule rule) throws QueryParseException {
		
		List<TreeNode> results = new ArrayList<TreeNode>();
		
		if (treeNode.hasChildren()) {
			
			for (TreeNode child : treeNode.children) {
				
				List<TreeNode> childMatches = descendantsOrSelfMatch(child, regex);
				
				if (childMatches != null) {
				
					results.addAll(childMatches);
				}
			}
			
		} else {
			
			String label = treeNode.getLabel();
			
			if (label != null && label.matches(regex)) {
				
				results.add(treeNode);
			}
		}
		
		for (TreeNode result : results) {
			
			rule.parse(result);
		}
	}
	
	public static void forAllDescendantsOrSelfThatMatchStream(TreeNode treeNode, String regex, QueryParserRule rule) throws QueryParseException {
		
		List<TreeNode> results = new ArrayList<TreeNode>();
		
		if (treeNode.hasChildren()) {
			
			forEach(treeNode.children.stream(),
					child -> forAllDescendantsOrSelfThatMatchStream(child, regex, rule));
			
		} else {
			
			String label = treeNode.getLabel();
			
			if (label != null && label.matches(regex)) {
				
				rule.parse(treeNode);
			}
		}
	}
	
	public static void forEach(Stream<TreeNode> stream, QueryParserRule rule) throws QueryParseException {
		
		for (Iterator<TreeNode> it = stream.iterator() ; it.hasNext() ;) {
			
			TreeNode node = it.next();
			rule.parse(node);
		}
	}

}
