package spade.query.quickgrail.execution;

import java.util.ArrayList;

import spade.query.quickgrail.entities.Graph;
import spade.query.quickgrail.utility.TreeStringSerializable;
import spade.storage.Quickstep;

public class InsertLiteralVertex extends Instruction {
  private Graph targetGraph;
  private ArrayList<String> vertices;

  public InsertLiteralVertex(Graph targetGraph, ArrayList<String> vertices) {
    this.targetGraph = targetGraph;
    this.vertices = vertices;
  }

  @Override
  public void execute(ExecutionContext ctx) {
    Quickstep qs = ctx.getQuickstepInstance();
    String prefix = "INSERT INTO " + targetGraph.getVertexTableName() + " VALUES(";
    StringBuilder sqlQuery = new StringBuilder();
    for (String vertex : vertices) {
      sqlQuery.append(prefix + vertex + ");\n");
    }
    qs.execute(sqlQuery.toString());
  }

  @Override
  public String getLabel() {
    return "InsertLiteralVertex";
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
    inline_field_names.add("vertices");
    inline_field_values.add("{" + String.join(",", vertices) + "}");
  }
}
