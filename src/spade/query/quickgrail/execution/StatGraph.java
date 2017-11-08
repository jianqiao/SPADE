package spade.query.quickgrail.execution;

import java.util.ArrayList;

import spade.query.quickgrail.entities.Graph;
import spade.query.quickgrail.utility.TreeStringSerializable;
import spade.storage.Quickstep;

public class StatGraph extends Instruction {
  private Graph targetGraph;

  public StatGraph(Graph targetGraph) {
    this.targetGraph = targetGraph;
  }

  @Override
  public void execute(ExecutionContext ctx) {
    Quickstep qs = ctx.getQuickstepInstance();

    String targetVertexTable = targetGraph.getVertexTableName();
    String targetEdgeTable = targetGraph.getEdgeTableName();
    long numVertices = qs.executeForLongResult(
        "COPY SELECT COUNT(*) FROM " + targetVertexTable + " TO stdout;");
    long numEdges = qs.executeForLongResult(
        "COPY SELECT COUNT(*) FROM " + targetEdgeTable + " TO stdout;");

    String stat = "# vertices = " + numVertices + ", # edges = " + numEdges;
    ctx.addResponse(stat);
  }

  @Override
  public String getLabel() {
    return "StatGraph";
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
  }
}
