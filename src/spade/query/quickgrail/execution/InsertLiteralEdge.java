package spade.query.quickgrail.execution;

import java.util.ArrayList;

import spade.query.quickgrail.entities.Graph;
import spade.query.quickgrail.utility.TreeStringSerializable;
import spade.storage.Quickstep;

public class InsertLiteralEdge extends Instruction {
  private Graph targetGraph;
  private ArrayList<String> edges;

  public InsertLiteralEdge(Graph targetGraph, ArrayList<String> edges) {
    this.targetGraph = targetGraph;
    this.edges = edges;
  }

  @Override
  public void execute(ExecutionContext ctx) {
    Quickstep qs = ctx.getQuickstepInstance();
    String prefix = "INSERT INTO " + targetGraph.getEdgeTableName() + " VALUES(";
    StringBuilder sqlQuery = new StringBuilder();
    for (String edge : edges) {
      sqlQuery.append(prefix + edge + ");\n");
    }
    qs.execute(sqlQuery.toString());
  }

  @Override
  public String getLabel() {
    return "InsertLiteralEdge";
  }

  @Override
  protected void getFieldStringItems(
      ArrayList<String> inline_field_names,
      ArrayList<String> inline_field_values,
      ArrayList<String> non_container_child_field_names,
      ArrayList<TreeStringSerializable> non_container_child_fields,
      ArrayList<String> container_child_field_names,
      ArrayList<ArrayList<? extends TreeStringSerializable>> container_child_fields) {
    inline_field_names.add("targetGraph");
    inline_field_values.add(targetGraph.getName());
    inline_field_names.add("edges");
    inline_field_values.add("{" + String.join(",", edges) + "}");
  }
}
