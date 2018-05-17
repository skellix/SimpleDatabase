package com.skellix.database.table.query;

import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.skellix.database.table.query.exception.QueryParseException;
import com.skellix.database.table.query.node.AddRowQueryNode;
import com.skellix.database.table.query.node.AliasTableQueryNode;
import com.skellix.database.table.query.node.AndQueryNode;
import com.skellix.database.table.query.node.DoubleNumberQueryNode;
import com.skellix.database.table.query.node.EntryQueryNode;
import com.skellix.database.table.query.node.EqualsQueryNode;
import com.skellix.database.table.query.node.GreaterThanQueryNode;
import com.skellix.database.table.query.node.BlockQueryNode;
import com.skellix.database.table.query.node.IntegerNumberQueryNode;
import com.skellix.database.table.query.node.JoinQueryNode;
import com.skellix.database.table.query.node.LessThanOrEqualsQueryNode;
import com.skellix.database.table.query.node.LessThanQueryNode;
import com.skellix.database.table.query.node.LimitQueryNode;
import com.skellix.database.table.query.node.ListQueryNode;
import com.skellix.database.table.query.node.MapQueryNode;
import com.skellix.database.table.query.node.NextQueryNodes;
import com.skellix.database.table.query.node.NotEqualsQueryNode;
import com.skellix.database.table.query.node.NotQueryNode;
import com.skellix.database.table.query.node.OrQueryNode;
import com.skellix.database.table.query.node.PreviousQueryNodes;
import com.skellix.database.table.query.node.QueryNode;
import com.skellix.database.table.query.node.SelectQueryNode;
import com.skellix.database.table.query.node.StringQueryNode;
import com.skellix.database.table.query.node.TableQueryNode;
import com.skellix.database.table.query.node.VariablePathQueryNode;
import com.skellix.database.table.query.node.WhereQueryNode;

import treeparser.TreeNode;
import treeparser.TreeParser;
import treeparser.io.IOSource;

public class QueryNodeParser {
	
	public static QueryNode parse(String source) throws QueryParseException {
		
		TreeNode tree = parseTree(source);
		
		TreeNode firstChild = tree.getFirstChild();
		
		if (firstChild instanceof QueryNode) {
			
			return (QueryNode) firstChild;
		}
		return null;
	}
	
	public static TreeNode parseTree(String source) throws QueryParseException {
		
		TreeNode tree = TreeParser.parse(source);
		
		parseTree(tree);
		
		return tree;
	}
	
	private static void parseTree(TreeNode tree) throws QueryParseException {
		
		parseBlockRule.parse(tree);
	}
	
	private static QueryParserRule parseFloatingPointNumbersAndVariablesRule = (tree) -> {
		
		forAllDescendantsOrSelfThatMatch(tree, "^\\.$",
				match -> {
					
					TreeNode previousNode = PreviousQueryNodes.getTypes(match, TreeNode.class).get(0);
					TreeNode nextNode = NextQueryNodes.getTypes(match, TreeNode.class).get(0);
					
					String previousString = previousNode.getLabel();
					String nextString = nextNode.getLabel();
					
					if (previousString.matches("^(?:\\-|\\+|)\\d+$") && nextString.matches("^\\d+$")) {
					
						try {
							
							String str = previousString + "." + nextString;
							IOSource source = new IOSource(MappedByteBuffer.wrap(str.getBytes()));
							TreeNode node = new TreeNode(source, 0, (int) source.buffer.limit() - 1, 0);
							
							new DoubleNumberQueryNode().parse(node);
							
						} catch (NumberFormatException e) {
							
							String errorString = String.format("ERROR: unable to parse number '%s' at %d, %d"
									, match.getLabel(), match.line, match.getStartColumn());
							
							throw new QueryParseException(errorString);
						}
					} else {
						
						VariablePathQueryNode variablePathQueryNode = new VariablePathQueryNode();
						variablePathQueryNode.copyValuesFrom(match);
						match.replaceWith(variablePathQueryNode);
						variablePathQueryNode.pathOf(previousNode, nextNode);
					}
					
					previousNode.removeFromParent();
					nextNode.removeFromParent();
				});
	};
	
	private static QueryParserRule parseIntegerNumbersRule = (tree) -> {
		
		forChildrenThatMatch(tree, "^(?:\\-|\\+|)\\d+$",
				match -> new IntegerNumberQueryNode().parse(match));
	};
	
	private static QueryParserRule parseNumbersAndVariablesRule = (tree) -> {
		
		parseFloatingPointNumbersAndVariablesRule.parse(tree);
		parseIntegerNumbersRule.parse(tree);
	};
	
	private static QueryParserRule parseStringsRule = (tree) -> {
		
		forChildrenThatMatch(tree, "^(['\"]).*\\1$",
				match -> new StringQueryNode().parse(match));
	};
	
	private static QueryParserRule parseSingleTokensRule = (tree) -> {
		
		parseNumbersAndVariablesRule.parse(tree);
		parseStringsRule.parse(tree);
	};
	
	private static QueryParserRule parseEntriesRule = (tree) -> {
		
		forChildrenThatMatch(tree, "^:$",
				match -> new EntryQueryNode().parse(match));
	};
	
	private static QueryParserRule parseEqualsRule = (tree) -> {
		
		forChildrenThatMatch(tree, "^==$",
				match -> new EqualsQueryNode().parse(match));
	};
	
	private static QueryParserRule parseNotEqualsRule = (tree) -> {
		
		forChildrenThatMatch(tree, "^!=$",
				match -> new NotEqualsQueryNode().parse(match));
	};
	
	private static QueryParserRule parseLessThanRule = (tree) -> {
		
		forChildrenThatMatch(tree, "^<$",
				match -> new LessThanQueryNode().parse(match));
	};
	
	private static QueryParserRule parseLessThanOrEqualRule = (tree) -> {
		
		forChildrenThatMatch(tree, "^<=$",
				match -> new LessThanOrEqualsQueryNode().parse(match));
	};
	
	private static QueryParserRule parseGreaterThanRule = (tree) -> {
		
		forChildrenThatMatch(tree, "^>$",
				match -> new GreaterThanQueryNode().parse(match));
	};
	
	private static QueryParserRule parseGreaterThanOrEqualRule = (tree) -> {
		
		forChildrenThatMatch(tree, "^>=$",
				match -> new GreaterThanQueryNode().parse(match));
	};
	
	private static QueryParserRule parseNotRule = (tree) -> {
		
		forChildrenThatMatch(tree, "^!$",
				match -> new NotQueryNode().parse(match));
	};
	
	private static QueryParserRule parseAndRule = (tree) -> {
		
		forChildrenThatMatch(tree, "^&&$",
				match -> new AndQueryNode().parse(match));
	};
	
	private static QueryParserRule parseOrRule = (tree) -> {
		
		forChildrenThatMatch(tree, "^\\|\\|$",
				match -> new OrQueryNode().parse(match));
	};
	
	private static QueryParserRule parseAliasRule = (tree) -> {
		
		forChildrenThatMatch(tree, "^as$",
				match -> new AliasTableQueryNode().parse(match));
	};
	
	private static QueryParserRule parseSimpleLeftRightTokensRule = (tree) -> {
		
		parseEntriesRule.parse(tree);
		parseEqualsRule.parse(tree);
		parseNotEqualsRule.parse(tree);
		parseLessThanRule.parse(tree);
		parseLessThanOrEqualRule.parse(tree);
		parseGreaterThanRule.parse(tree);
		parseGreaterThanOrEqualRule.parse(tree);
		parseNotRule.parse(tree);
		parseAndRule.parse(tree);
		parseOrRule.parse(tree);
		parseAliasRule.parse(tree);
	};
	
	private static QueryParserRule parseTableTokensRule = (tree) -> {
		
		forChildrenThatMatch(tree, "^table$",
				match -> new TableQueryNode().parse(match));
	};
	
	private static QueryParserRule parseSimpleRightTokensRule = (tree) -> {
		
		parseTableTokensRule.parse(tree);
	};
	
	private static QueryParserRule parseMapRule = (tree) -> {
		
		List<TreeNode> list = childrenThatHaveChildren(tree);
		
		for (TreeNode match : list) {
			
			String enterLabel = match.getEnterLabel();
			
			if (enterLabel != null && enterLabel.equals("{")) {
				
				new MapQueryNode().parse(match);
			}
		}
	};
	
	private static QueryParserRule parseAddRowRule = (tree) -> {
		
		forChildrenThatMatch(tree, "^addRow$",
				match -> new AddRowQueryNode().parse(match));
	};
	
	private static QueryParserRule parseJoinRule = (tree) -> {
		
		forChildrenThatMatch(tree, "^join$",
				match -> new JoinQueryNode().parse(match));
	};
	
	private static QueryParserRule parseWhereRule = (tree) -> {
		
		forChildrenThatMatch(tree, "^where$",
				match -> new WhereQueryNode().parse(match));
	};
	
	private static QueryParserRule parseLimitRule = (tree) -> {
		
		forChildrenThatMatch(tree, "^limit$",
				match -> new LimitQueryNode().parse(match));
	};
	
	private static QueryParserRule parseListRule = (tree) -> {
		
		forChildrenThatMatch(tree, "^,$",
				match -> new ListQueryNode().parse(match));
	};
	
	private static QueryParserRule parseSelectRule = (tree) -> {
		
		forChildrenThatMatch(tree, "^select$",
				match -> new SelectQueryNode().parse(match));
	};
	
	private static QueryParserRule parseComplexTokensRule = (tree) -> {
		
		parseMapRule.parse(tree);
		parseAddRowRule.parse(tree);
		parseJoinRule.parse(tree);
		parseWhereRule.parse(tree);
		parseListRule.parse(tree);
		parseSelectRule.parse(tree);
		parseLimitRule.parse(tree);
	};
	
	private static QueryParserRule parseBlocksRule = null;
	
	private static QueryParserRule parseBlockRule = (tree) -> {
		
		parseBlocksRule.parse(tree);
		parseSingleTokensRule.parse(tree);
		parseSimpleRightTokensRule.parse(tree);
		parseSimpleLeftRightTokensRule.parse(tree);
		parseComplexTokensRule.parse(tree);
		
		String enterLabel = tree.getEnterLabel();
		
		if (enterLabel != null) {
			
			if (enterLabel.equals("(")) {
			
				new BlockQueryNode().parse(tree);
				
			} else if (enterLabel.equals("{")) {
				
				new MapQueryNode().parse(tree);
			}
		}
	};
	
	static {
		parseBlocksRule = (tree) -> {
			
			List<TreeNode> list = childrenThatHaveChildren(tree);
			
			for (TreeNode match : list) {
				
				parseBlockRule.parse(match);
			}
		};
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

	private static void forChildrenThatMatch(TreeNode treeNode, String regex, QueryParserRule rule) throws QueryParseException {
		
		if (treeNode.hasChildren()) {
			
			List<TreeNode> matches = treeNode.children.stream().filter(child -> {
				
				String label = child.getLabel();
				return label != null && label.matches(regex);
				
			}).collect(Collectors.toList());
			
			forEach(matches.stream(), child -> rule.parse(child));
		}
	}

	private static List<TreeNode> childrenThatHaveChildren(TreeNode treeNode) {
		
		if (treeNode.hasChildren()) {
			
			List<TreeNode> matches = treeNode.children.stream()
					.filter(child -> child != null && child.hasChildren())
					.collect(Collectors.toList());
			
			return matches;
		}
		
		return Arrays.asList();
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
		
		return Arrays.asList();
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
	
	public static Stream<TreeNode> forAllDescendantsOrSelf(TreeNode treeNode) throws QueryParseException {
		
		if (treeNode != null && treeNode.hasChildren()) {
			
			return Stream.concat(
					treeNode.children.stream().flatMap(child -> {
						try {
							return forAllDescendantsOrSelf(child);
						} catch (QueryParseException e) {
							e.printStackTrace();
						}
						return Stream.of();
					}),
					Stream.of(treeNode));
		}
		
		return Stream.of(treeNode);
	}

}
