package spade.query.quickgrail.execution;

import java.util.ArrayList;

import spade.query.quickgrail.entities.Graph;
import spade.query.quickgrail.utility.TreeStringSerializable;
import spade.storage.Quickstep;

public class UnionGraph extends Instruction {
  private Graph targetGraph;
  private Graph sourceGraph;

  public UnionGraph(Graph targetGraph, Graph sourceGraph) {
    this.targetGraph = targetGraph;
    this.sourceGraph = sourceGraph;
  }

  @Override
  public void execute(ExecutionContext ctx) {
    String sourceVertexTable = sourceGraph.getVertexTableName();
    String sourceEdgeTable = sourceGraph.getEdgeTableName();
    String targetVertexTable = targetGraph.getVertexTableName();
    String targetEdgeTable = targetGraph.getEdgeTableName();

    Quickstep qs = ctx.getQuickstepInstance();
    qs.execute("INSERT INTO " + targetVertexTable +
               " SELECT id FROM " + sourceVertexTable + ";");
    qs.execute("INSERT INTO " + targetEdgeTable +
               " SELECT id FROM " + sourceEdgeTable + ";");
  }

  @Override
  public String getLabel() {
    return "UnionGraph";
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
    inline_field_names.add("sourceGraph");
    inline_field_values.add(sourceGraph.getName());
  }
}
