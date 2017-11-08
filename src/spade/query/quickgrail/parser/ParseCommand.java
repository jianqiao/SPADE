package spade.query.quickgrail.parser;

import java.util.ArrayList;

import spade.query.quickgrail.utility.TreeStringSerializable;

public class ParseCommand extends ParseStatement {
  private ParseString commandName;
  private ArrayList<ParseExpression> arguments;

  public ParseCommand(int lineNumber, int columnNumber, ParseString commandName) {
    super(lineNumber, columnNumber, ParseStatement.StatementType.kCommand);
    this.commandName = commandName;
    this.arguments = new ArrayList<ParseExpression>();
  }

  public void addArgument(ParseExpression argument) {
    arguments.add(argument);
  }

  public ParseString getCommandName() {
    return commandName;
  }

  public ArrayList<ParseExpression> getArguments() {
    return arguments;
  }

  @Override
  public String getLabel() {
    return "Command";
  }

  @Override
  protected void getFieldStringItems(
      ArrayList<String> inline_field_names,
      ArrayList<String> inline_field_values,
      ArrayList<String> non_container_child_field_names,
      ArrayList<TreeStringSerializable> non_container_child_fields,
      ArrayList<String> container_child_field_names,
      ArrayList<ArrayList<? extends TreeStringSerializable>> container_child_fields) {
    inline_field_names.add("commandName");
    inline_field_values.add(commandName.getValue());
    container_child_field_names.add("arguments");
    container_child_fields.add(arguments);
  }
}
