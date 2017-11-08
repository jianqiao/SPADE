package spade.query.quickgrail.execution;

import java.util.ArrayList;

import spade.query.quickgrail.utility.TreeStringSerializable;

public class EvaluateQuery extends Instruction {
  private String sqlQuery;

  public EvaluateQuery(String sqlQuery) {
    this.sqlQuery = sqlQuery;
  }

  @Override
  public String getLabel() {
    return "EvaluateQuery";
  }

  @Override
  public void execute(ExecutionContext ctx) {
    ctx.getQuickstepInstance().execute(sqlQuery);
  }

  @Override
  protected void getFieldStringItems(
      ArrayList<String> inline_field_names,
      ArrayList<String> inline_field_values,
      ArrayList<String> non_container_child_field_names,
      ArrayList<TreeStringSerializable> non_container_child_fields,
      ArrayList<String> container_child_field_names,
      ArrayList<ArrayList<? extends TreeStringSerializable>> container_child_fields) {
    inline_field_names.add("sqlQuery");
    inline_field_values.add(sqlQuery);
  }
}
