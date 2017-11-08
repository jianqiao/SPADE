package spade.query.quickgrail.parser;

import spade.query.quickgrail.utility.TreeStringSerializable;

public abstract class ParseTreeNode extends TreeStringSerializable {
  private int lineNumber;
  private int columnNumber;

  public ParseTreeNode(int lineNumber, int columnNumber) {
    this.lineNumber = lineNumber;
    this.columnNumber = columnNumber;
  }

  public int getLineNumber() {
    return lineNumber;
  }

  public int getColumnNumber() {
  	  return columnNumber;
  }

  public String getLocationString() {
    return "line " + lineNumber + ", column " + columnNumber;
  }
}
