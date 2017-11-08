package spade.query.quickgrail.parser;

import java.util.ArrayList;

import spade.query.quickgrail.utility.TreeStringSerializable;

public class ParseString extends ParseTreeNode {
  private String value;

  public ParseString(int lineNumber, int columnNumber, String value) {
    super(lineNumber, columnNumber);
    this.value = value;
  }

  public String getValue() {
    return value;
  }

  @Override
  public String getLabel() {
    return "String";
  }

  @Override
  protected void getFieldStringItems(
      ArrayList<String> inline_field_names,
      ArrayList<String> inline_field_values,
      ArrayList<String> non_container_child_field_names,
      ArrayList<TreeStringSerializable> non_container_child_fields,
      ArrayList<String> container_child_field_names,
      ArrayList<ArrayList<? extends TreeStringSerializable>> container_child_fields) {
    inline_field_names.add("value");
    inline_field_values.add(value);
  }
}
