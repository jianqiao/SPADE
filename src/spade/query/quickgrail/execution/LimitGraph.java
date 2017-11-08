package spade.query.quickgrail.execution;

import java.util.ArrayList;

import spade.query.quickgrail.entities.Graph;
import spade.query.quickgrail.utility.TreeStringSerializable;
import spade.storage.Quickstep;

public class LimitGraph extends Instruction {
  private Graph targetGraph;
  private Graph sourceGraph;
  private int limit;

  public LimitGraph(Graph targetGraph, Graph sourceGraph, int limit) {
    this.targetGraph = targetGraph;
    this.sourceGraph = sourceGraph;
    this.limit = limit;
  }

  @Override
  public void execute(ExecutionContext ctx) {
    Quickstep qs = ctx.getQuickstepInstance();

    String sourceVertexTable = sourceGraph.getVertexTableName();
    String sourceEdgeTable = sourceGraph.getEdgeTableName();

    long numVertices = qs.executeForLongResult(
        "COPY SELECT COUNT(*) FROM " + sourceVertexTable + " TO stdout;");
    long numEdges = qs.executeForLongResult(
        "COPY SELECT COUNT(*) FROM " + sourceEdgeTable + " TO stdout;");

    if (numVertices > 0) {
      qs.execute("\\analyzerange " + sourceVertexTable + "\n" +
                 "INSERT INTO " + targetGraph.getVertexTableName() +
                 " SELECT id FROM " + sourceVertexTable + " GROUP BY id" +
                 " ORDER BY id LIMIT " + limit + ";");

    }
    if (numEdges > 0) {
      qs.execute("\\analyzerange " + sourceEdgeTable + "\n" +
                 "INSERT INTO " + targetGraph.getEdgeTableName() +
                 " SELECT id FROM " + sourceEdgeTable + " GROUP BY id" +
                 " ORDER BY id LIMIT " + limit + ";");
    }
  }

  @Override
  public String getLabel() {
    return "LimitGraph";
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
    inline_field_names.add("limit");
    inline_field_values.add(String.valueOf(limit));
  }
}
