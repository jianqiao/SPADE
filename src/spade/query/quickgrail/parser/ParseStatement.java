package spade.query.quickgrail.parser;

public abstract class ParseStatement extends ParseTreeNode {
  public enum StatementType {
    kAssignment,
    kCommand
  }

  private StatementType statementType;

  public ParseStatement(int lineNumber, int columnNumber,
                        StatementType statementType) {
    super(lineNumber, columnNumber);
    this.statementType = statementType;
  }

  public StatementType getStatementType() {
    return statementType;
  }
}
