package spade.query.quickgrail.execution;

import java.util.ArrayList;

import spade.query.quickgrail.entities.Graph;
import spade.query.quickgrail.utility.TreeStringSerializable;
import spade.storage.Quickstep;

public class SubtractGraph extends Instruction {
  private Graph outputGraph;
  private Graph minuendGraph;
  private Graph subtrahendGraph;

  public SubtractGraph(Graph outputGraph,  Graph minuendGraph, Graph subtrahendGraph) {
    this.outputGraph = outputGraph;
    this.minuendGraph = minuendGraph;
    this.subtrahendGraph = subtrahendGraph;
  }

  @Override
  public void execute(ExecutionContext ctx) {
    String outputVertexTable = outputGraph.getVertexTableName();
    String outputEdgeTable = outputGraph.getEdgeTableName();
    String minuendVertexTable = minuendGraph.getVertexTableName();
    String minuendEdgeTable = minuendGraph.getEdgeTableName();
    String subtrahendVertexTable = subtrahendGraph.getVertexTableName();
    String subtrahendEdgeTable = subtrahendGraph.getEdgeTableName();

    Quickstep qs = ctx.getQuickstepInstance();
    qs.execute("\\analyzerange " + subtrahendVertexTable + " " + subtrahendEdgeTable + "\n");
    qs.execute("INSERT INTO " + outputVertexTable +
               " SELECT id FROM " + minuendVertexTable +
               " WHERE id NOT IN (SELECT id FROM " + subtrahendVertexTable + ");");
    qs.execute("INSERT INTO " + outputEdgeTable +
               " SELECT id FROM " + minuendEdgeTable +
               " WHERE id NOT IN (SELECT id FROM " + subtrahendEdgeTable + ");");
  }

  @Override
  public String getLabel() {
    return "SubtractGraph";
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
    inline_field_names.add("minuendGraph");
    inline_field_values.add(minuendGraph.getName());
    inline_field_names.add("subtrahendGraph");
    inline_field_values.add(subtrahendGraph.getName());
  }
}
