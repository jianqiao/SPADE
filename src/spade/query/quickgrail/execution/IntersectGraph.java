package spade.query.quickgrail.execution;

import java.util.ArrayList;

import spade.query.quickgrail.entities.Graph;
import spade.query.quickgrail.utility.TreeStringSerializable;
import spade.storage.Quickstep;

public class IntersectGraph extends Instruction {
  private Graph outputGraph;
  private Graph lhsGraph;
  private Graph rhsGraph;

  public IntersectGraph(Graph outputGraph, Graph lhsGraph, Graph rhsGraph) {
    this.outputGraph = outputGraph;
    this.lhsGraph = lhsGraph;
    this.rhsGraph = rhsGraph;
  }

  @Override
  public void execute(ExecutionContext ctx) {
    String outputVertexTable = outputGraph.getVertexTableName();
    String outputEdgeTable = outputGraph.getEdgeTableName();
    String lhsVertexTable = lhsGraph.getVertexTableName();
    String lhsEdgeTable = lhsGraph.getEdgeTableName();
    String rhsVertexTable = rhsGraph.getVertexTableName();
    String rhsEdgeTable = rhsGraph.getEdgeTableName();

    Quickstep qs = ctx.getQuickstepInstance();
    qs.execute("\\analyzerange " + rhsVertexTable + " " + rhsEdgeTable + "\n");
    qs.execute("INSERT INTO " + outputVertexTable +
               " SELECT id FROM " + lhsVertexTable +
               " WHERE id IN (SELECT id FROM " + rhsVertexTable + ");");
    qs.execute("INSERT INTO " + outputEdgeTable +
               " SELECT id FROM " + lhsEdgeTable +
               " WHERE id IN (SELECT id FROM " + rhsEdgeTable + ");");
  }

  @Override
  public String getLabel() {
    return "IntersectGraph";
  }

  @Override
  protected void getFieldStringItems(
      ArrayList<String> inline_field_names,
      ArrayList<String> inline_field_values,
      ArrayList<String> non_container_child_field_names,
      ArrayList<TreeStringSerializable> non_container_child_fields,
      ArrayList<String> container_child_field_names,
      ArrayList<ArrayList<? extends TreeStringSerializable>> container_child_fields) {
    inline_field_names.add("outputGraph");
    inline_field_values.add(outputGraph.getName());
    inline_field_names.add("lhsGraph");
    inline_field_values.add(lhsGraph.getName());
    inline_field_names.add("rhsGraph");
    inline_field_values.add(rhsGraph.getName());
  }
}
