package com.skellix.database.table.query.node;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import com.skellix.database.table.query.exception.QueryParseException;

import treeparser.TreeNode;

public class NextQueryNodes {

	@SafeVarargs
	public static List<TreeNode> getTypes(TreeNode replaceNode, Class<? extends TreeNode> ... nodeClasses) throws QueryParseException {
		
		List<TreeNode> output = new ArrayList<>();
		TreeNode lastNode = replaceNode;
		TreeNode next = lastNode.getNextSibling();
		
		for (Class<? extends TreeNode> clazz : nodeClasses) {
			
			if (next == null) {
				
				throw QueryParseException.format("Expected %s but found null", getTypeName(clazz));
			}
			
			if (!clazz.isInstance(next)) {
				
				throw QueryParseException.format("Expected %s but found %s", getTypeName(clazz), next.toString());
			}
			
			output.add(next);
			lastNode = next;
			next = lastNode.getNextSibling();
			lastNode.parent.children.remove(lastNode);
		}
		
		return output;
	}

	private static String getTypeName(Class<? extends TreeNode> clazz) {
		
		String expectedName = "value";
		
		if (clazz.isAssignableFrom(QueryNode.class)) {
			
			try {
				Object result = clazz.getDeclaredMethod("getTypeName").invoke(clazz.newInstance());
				
				if (result instanceof String) {
					
					String typeName = (String) result;
					expectedName = typeName;
				}
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			} catch (IllegalArgumentException e) {
				e.printStackTrace();
			} catch (InvocationTargetException e) {
				e.printStackTrace();
			} catch (NoSuchMethodException e) {
				e.printStackTrace();
			} catch (SecurityException e) {
				e.printStackTrace();
			} catch (InstantiationException e) {
				e.printStackTrace();
			}
		}
		return expectedName;
	}

}
