package spade.query.quickgrail.execution;

import java.util.ArrayList;

import spade.query.quickgrail.entities.Graph;
import spade.query.quickgrail.utility.TreeStringSerializable;
import spade.storage.Quickstep;

public class CollapseEdge extends Instruction {
  private Graph targetGraph;
  private Graph sourceGraph;
  private ArrayList<String> fields;

  public CollapseEdge(Graph targetGraph, Graph sourceGraph, ArrayList<String> fields) {
    this.targetGraph = targetGraph;
    this.sourceGraph = sourceGraph;
    this.fields = fields;
  }

  @Override
  public void execute(ExecutionContext ctx) {
    String sourceVertexTable = sourceGraph.getVertexTableName();
    String sourceEdgeTable = sourceGraph.getEdgeTableName();
    String targetVertexTable = targetGraph.getVertexTableName();
    String targetEdgeTable = targetGraph.getEdgeTableName();

    Quickstep qs = ctx.getQuickstepInstance();
    qs.execute("\\analyzerange " + sourceVertexTable + " " + sourceEdgeTable + "\n");
    qs.execute("INSERT INTO " + targetVertexTable +
               " SELECT id FROM " + sourceVertexTable + ";");

    StringBuilder tables = new StringBuilder();
    StringBuilder predicates = new StringBuilder();
    StringBuilder groups = new StringBuilder();

    for (int i = 0; i < fields.size(); ++i) {
      String edgeAnnoName = "ea" + i;
      tables.append(", edge_anno " + edgeAnnoName);
      predicates.append(" AND e.id = " + edgeAnnoName + ".id" +
                        " AND " + edgeAnnoName + ".field = '" + fields.get(i) + "'");
      groups.append(", " + edgeAnnoName + ".value");
    }

    qs.execute("INSERT INTO " + targetEdgeTable +
               " SELECT MIN(e.id) FROM edge e" + tables.toString() +
               " WHERE e.id IN (SELECT id FROM " + sourceEdgeTable + ")" + predicates.toString() +
               " GROUP BY src, dst" + groups.toString() + ";");
  }

  @Override
  public String getLabel() {
    return "CollapseEdge";
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
