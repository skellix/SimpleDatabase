package com.skellix.database.table.query.node;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import com.skellix.database.table.query.exception.QueryParseException;

import treeparser.TreeNode;

public class PreviousQueryNodes {

	@SafeVarargs
	public static List<TreeNode> getTypes(TreeNode replaceNode, Class<? extends TreeNode> ... nodeClasses) throws QueryParseException {
		
		List<TreeNode> output = new ArrayList<>();
		TreeNode lastNode = replaceNode;
		TreeNode previous = lastNode.getPreviousSibling();
		
		for (Class<? extends TreeNode> clazz : nodeClasses) {
			
			if (previous == null) {
				
				throw QueryParseException.format("Expected %s but found null", getTypeName(clazz));
			}
			
			if (!clazz.isInstance(previous)) {
				
				throw QueryParseException.format("Expected %s but found %s", getTypeName(clazz), previous.toString());
			}
			
			output.add(previous);
			lastNode = previous;
			lastNode.parent.children.remove(lastNode);
			previous = lastNode.getPreviousSibling();
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
