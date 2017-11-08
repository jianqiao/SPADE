package spade.query.quickgrail.parser;

import java.util.ArrayList;

import spade.query.quickgrail.utility.TreeStringSerializable;

public class ParseProgram extends ParseTreeNode {
  private ArrayList<ParseStatement> statements;

  public ParseProgram(int lineNumber, int columnNumber) {
    super(lineNumber, columnNumber);
    this.statements = new ArrayList<ParseStatement>();
  }

  @Override
  public String getLabel() {
    return "Program";
  }

  public void addStatement(ParseStatement statement) {
    statements.add(statement);
  }

  public ArrayList<ParseStatement> getStatements() {
    return statements;
  }

  @Override
  protected void getFieldStringItems(
      ArrayList<String> inline_field_names,
      ArrayList<String> inline_field_values,
      ArrayList<String> non_container_child_field_names,
      ArrayList<TreeStringSerializable> non_container_child_fields,
      ArrayList<String> container_child_field_names,
      ArrayList<ArrayList<? extends TreeStringSerializable>> container_child_fields) {
    container_child_field_names.add("statements");
    container_child_fields.add(statements);
  }
}
