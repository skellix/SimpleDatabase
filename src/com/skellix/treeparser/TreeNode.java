package com.skellix.treeparser;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 
 * @author Alexander Jones <24k911@gmail.com"> 
 *
 */
public class TreeNode {

	TreeNode parent = null;
	public ByteBuffer source = null;
	public int line = -1;
	public int exitLine = -1;
	public int start = -1;
	public int enter = -1;
	public int exit = -1;
	public int end = -1;
	public ArrayList<TreeNode> children = new ArrayList<TreeNode>();

	public TreeNode() {
		// TODO Auto-generated constructor stub
	}

	public TreeNode(ByteBuffer input, int start) {
		this.source = input;
		this.start = start;
	}

	public TreeNode(ByteBuffer source, int start, int end, int line) {
		this.source = source;
		this.start = start;
		this.end = end;
		this.line = line;
	}

	public TreeNode(ByteBuffer source, int start, int enter, int exit, int end, int line, int exitLine) {
		this.source = source;
		this.start = start;
		this.enter = enter;
		this.exit = exit;
		this.end = end;
		this.line = line;
		this.exitLine = exitLine;
	}
	
	public String getLabel() {
		
		int length = (end - start) + 1;
		
		if ((start + length) > source.limit() + 1) {
			return null;
		}
		
		byte[] data = new byte[length];
		source.position(start);
		source.get(data, 0, length);
		
		return new String(data);
	}
	
	public String getEnterLabel() {
		
		int length = (enter - start) + 1;
		
		if (start + length > source.limit()) {
			return null;
		}
		
		byte[] data = new byte[length];
		source.position(start);
		source.get(data, 0, length);
		
		return new String(data);
	}
	
	public String getExitLabel() {
		
		int length = (end - exit) + 1;
		
		if (exit + length > source.limit()) {
			return null;
		}
		
		byte[] data = new byte[length];
		source.position(exit);
		source.get(data, 0, length);
		
		return new String(data);
	}
	
	public int getStartColumn() {
		
		if (start == -1) {
			return 0;
		}
		
		for (int i = start ; i >= 0 ; i --) {
			source.position(i);
			char c = source.getChar();
			if (i == 0) {
				return start;
			}
			if (c == '\n') {
				return start - i;
			}
		}
		return 0;
	}
	
	public boolean hasParent() {
		return parent != null;
	}
	
	public boolean isEmpty() {
		return start == -1;
	}
	
	public boolean hasChildren() {
		return children != null && !children.isEmpty();
	}
	
	public TreeNode getSiblingNode(int nodeIndex) {
		
		if (!hasParent()) {
			return null;
		}
		
		if (nodeIndex >= 0 && nodeIndex < parent.children.size()) {
			return parent.children.get(nodeIndex);
		}
		
		return null;
	}

	public void add(TreeNode childNode) {
		children.add(childNode);
		childNode.parent = this;
	}
	
	public void addAll(ArrayList<TreeNode> children) {
		for (TreeNode child : children) {
			add(child);
		}
	}

	public TreeNode cloneWithoutLinks() {
		return new TreeNode(source, start, enter, exit, end, line, exitLine);
	}
	
	public boolean matchesPattern(String pattern) {
		
		String text = null;
		
		if (this.start == -1) {
			
			System.err.printf("[ERROR] null node in comparison at line: %d\n", this.line);
			return false;
			
		} else if (this.enter == -1) {
			
			text = this.getLabel();
			
		} else {
			
			text = this.getEnterLabel();
		}
		
		Matcher matcher = Pattern.compile(pattern).matcher(text);
		
		return matcher.find();
	}

	public Collection<TreeNode> getChildrenMatchingPattern(String pattern) {
		ArrayList<TreeNode> matches = new ArrayList<TreeNode>();
		if (this.hasChildren()) {
			for (TreeNode child : children) {
				if (child.matchesPattern(pattern)) {
					matches.add(child);
				}
			}
		}
		return matches;
	}

	public Collection<TreeNode> getParentMatchingPattern(String pattern) {
		ArrayList<TreeNode> matches = new ArrayList<TreeNode>();
		if (this.hasParent()) {
			matches.add(parent);
		}
		return matches;
	}

	@Override
	public String toString() {
		return treeNodeToString(new AtomicInteger(-1));
	}
	
	protected String treeNodeToString(AtomicInteger line) {
		StringBuilder stringBuilder = new StringBuilder();
		if (start != -1) {
			if (this.line != line.get()) {
				if (line.get() != -1) {
					stringBuilder.append("\n");
				}
				line.set(this.line);
			}
			int i = this.start - 1;
			for (; i >= 0 ; i --) {
				if (!(this.source.get(i) == ' '
						|| this.source.get(i) == '\r'
						||this.source.get(i) == '\n'
						|| this.source.get(i) == '\t'
						)) {
					i ++;
					break;
				}
			}
			if (i >= 0) {
				int length = start - i;
				byte[] data = new byte[length];
				source.position(i);
				source.get(data, 0, length);
				stringBuilder.append(new String(data).replaceAll("\n", ""));
			}
			if (enter != -1) {
				stringBuilder.append(getEnterLabel().replaceAll("\n", ""));
			} else if (end != -1) {
				stringBuilder.append(getLabel().replaceAll("\n", ""));
			}
		}
		if (children.size() > 0) {
			for (TreeNode child : children) {
				stringBuilder.append(child.treeNodeToString(line));
			}
		}
		if (exit != -1) {
			if (this.exitLine != -1 && this.exitLine != line.get()) {
				line.set(this.exitLine);
				stringBuilder.append("\n");
			}
			int i = this.exit - 1;
			for (; i >= 0 ; i --) {
				if (!(this.source.get(i) == ' '
						|| this.source.get(i) == '\r'
						|| this.source.get(i) == '\n'
						|| this.source.get(i) == '\t'
						)) {
					i ++;
					break;
				}
			}
			int length = exit - i;
			byte[] data = new byte[length];
			source.position(i);
			source.get(data, 0, length);
			stringBuilder.append(new String(data).replaceAll("\n", ""));
			stringBuilder.append(getExitLabel().replaceAll("\n", ""));
		}
		return stringBuilder.toString();
	}
}
