package spade.query.quickgrail.parser;

import java.util.ArrayList;

import spade.query.quickgrail.types.Type;
import spade.query.quickgrail.utility.TreeStringSerializable;

public class ParseVariable extends ParseExpression {
  private ParseString name;
  private Type type;

  public ParseVariable(int lineNumber, int columnNumber,
                       ParseString name, Type type) {
    super(lineNumber, columnNumber, ParseExpression.ExpressionType.kVariable);
    this.name = name;
    this.type = type;
  }

  public ParseString getName() {
    return name;
  }

  public Type getType() {
    return type;
  }

  @Override
  public String getLabel() {
    return "Variable";
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
    inline_field_names.add("type");
    inline_field_values.add(type.getName());
  }
}
