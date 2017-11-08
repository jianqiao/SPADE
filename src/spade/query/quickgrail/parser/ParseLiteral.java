package spade.query.quickgrail.parser;

import java.util.ArrayList;

import spade.query.quickgrail.types.TypedValue;
import spade.query.quickgrail.utility.TreeStringSerializable;

public class ParseLiteral extends ParseExpression {
  private TypedValue literalValue;

  public ParseLiteral(int lineNumber, int columnNumber, TypedValue literalValue) {
    super(lineNumber, columnNumber, ParseExpression.ExpressionType.kLiteral);
    this.literalValue = literalValue;
  }

  public TypedValue getLiteralValue() {
    return literalValue;
  }

  @Override
  public String getLabel() {
    return "Literal";
  }

  @Override
  protected void getFieldStringItems(
      ArrayList<String> inline_field_names,
      ArrayList<String> inline_field_values,
      ArrayList<String> non_container_child_field_names,
      ArrayList<TreeStringSerializable> non_container_child_fields,
      ArrayList<String> container_child_field_names,
      ArrayList<ArrayList<? extends TreeStringSerializable>> container_child_fields) {
    inline_field_names.add("type");
    inline_field_values.add(literalValue.getType().getName());
    inline_field_names.add("value");
    inline_field_values.add(literalValue.toString());
  }
}
