package spade.query.quickgrail.parser;

import java.util.ArrayList;

import spade.query.quickgrail.utility.TreeStringSerializable;

public class ParseOperation extends ParseExpression {
  private ParseExpression subject;
  private ParseString operator;
  private ArrayList<ParseExpression> operands;

  public ParseOperation(int lineNumber, int columnNumber,
                        ParseExpression subject, ParseString operator) {
    super(lineNumber, columnNumber, ParseExpression.ExpressionType.kOperation);
    this.subject = subject;
    this.operator = operator;
    this.operands = new ArrayList<ParseExpression>();
  }

  public void addOperand(ParseExpression operand) {
    operands.add(operand);
  }

  public ParseExpression getSubject() {
    return subject;
  }

  public ParseString getOperator() {
    return operator;
  }

  public ArrayList<ParseExpression> getOperands() {
    return operands;
  }

  @Override
  public String getLabel() {
    return "Operation";
  }

  @Override
  protected void getFieldStringItems(
      ArrayList<String> inline_field_names,
      ArrayList<String> inline_field_values,
      ArrayList<String> non_container_child_field_names,
      ArrayList<TreeStringSerializable> non_container_child_fields,
      ArrayList<String> container_child_field_names,
      ArrayList<ArrayList<? extends TreeStringSerializable>> container_child_fields) {
    if (subject != null) {
      non_container_child_field_names.add("subject");
      non_container_child_fields.add(subject);
    }
    inline_field_names.add("operator");
    inline_field_values.add(operator.getValue());
    container_child_field_names.add("operands");
    container_child_fields.add(operands);
  }
}
