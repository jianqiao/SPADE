package spade.query.quickgrail.parser;

public abstract class ParseExpression extends ParseTreeNode {
  public enum ExpressionType {
    kLiteral,
    kName,
    kOperation,
    kVariable
  }

  private ExpressionType expressionType;

  public ParseExpression(int lineNumber, int columnNumber,
                         ExpressionType expressionType) {
    super(lineNumber, columnNumber);
    this.expressionType = expressionType;
  }

  public ExpressionType getExpressionType() {
    return expressionType;
  }
}
