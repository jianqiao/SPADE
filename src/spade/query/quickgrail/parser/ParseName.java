package spade.query.quickgrail.parser;

import java.util.ArrayList;

import spade.query.quickgrail.utility.TreeStringSerializable;

public class ParseName extends ParseExpression {
  private ParseString name;

  public ParseName(int lineNumber, int columnNumber, ParseString name) {
    super(lineNumber, columnNumber, ParseExpression.ExpressionType.kName);
    this.name = name;
  }

  @Override
  public String getLabel() {
    return "Name";
  }

  public ParseString getName() {
    return name;
  }

  @Override
  protected void getFieldStringItems(
      ArrayList<String> inline_field_names,
      ArrayList<String> inline_field_values,
      ArrayList<String> non_container_child_field_names,
      ArrayList<TreeStringSerializable> non_container_child_fields,
      ArrayList<String> container_child_field_names,
      ArrayList<ArrayList<? extends TreeStringSerializable>> container_child_fields) {
    inline_field_names.add("name");
    inline_field_values.add(name.getValue());
  }
}
