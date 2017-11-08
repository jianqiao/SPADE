package spade.query.quickgrail.execution;

import java.util.ArrayList;

import spade.query.quickgrail.entities.Graph;
import spade.query.quickgrail.utility.TreeStringSerializable;
import spade.storage.Quickstep;

public class GetSubgraph extends Instruction {
  private Graph targetGraph;
  private Graph subjectGraph;
  private Graph skeletonGraph;

  public GetSubgraph(Graph targetGraph, Graph subjectGraph, Graph skeletonGraph) {
    this.targetGraph = targetGraph;
    this.subjectGraph = subjectGraph;
    this.skeletonGraph = skeletonGraph;
  }

  @Override
  public void execute(ExecutionContext ctx) {
    Quickstep qs = ctx.getQuickstepInstance();

    String targetVertexTable = targetGraph.getVertexTableName();
    String targetEdgeTable = targetGraph.getEdgeTableName();
    String subjectVertexTable = subjectGraph.getVertexTableName();
    String subjectEdgeTable = subjectGraph.getEdgeTableName();
    String skeletonVertexTable = skeletonGraph.getVertexTableName();
    String skeletonEdgeTable = skeletonGraph.getEdgeTableName();

    qs.execute("DROP TABLE m_answer;\n" + "CREATE TABLE m_answer (id INT);");

    // Get vertices.
    qs.execute("\\analyzerange " + subjectVertexTable + "\n" +
               "INSERT INTO m_answer SELECT id FROM " + skeletonVertexTable +
               " WHERE id IN (SELECT id FROM " + subjectVertexTable + ");\n" +
               "INSERT INTO m_answer SELECT src FROM edge " +
               " WHERE id IN (SELECT id FROM " + skeletonEdgeTable + ")" +
               " AND src IN (SELECT id FROM " + subjectVertexTable + ");\n" +
               "INSERT INTO m_answer SELECT dst FROM edge" +
               " WHERE id IN (SELECT id FROM " + skeletonEdgeTable + ")" +
               " AND dst IN (SELECT id FROM " + subjectVertexTable + ");\n" +
               "\\analyzerange m_answer\n" +
               "INSERT INTO " + targetVertexTable + " SELECT id FROM m_answer GROUP BY id;\n");

    // Get edges.
    qs.execute("\\analyzerange " + subjectEdgeTable + "\n" +
               "INSERT INTO " + targetEdgeTable +
               " SELECT s.id FROM " + subjectEdgeTable + " s, edge e" +
               " WHERE s.id = e.id AND e.src IN (SELECT id FROM m_answer)" +
               " AND e.dst IN (SELECT id FROM m_answer) GROUP BY s.id;");

    qs.execute("DROP TABLE m_answer;");
  }

  @Override
  public String getLabel() {
    return "GetSubgraph";
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
    inline_field_names.add("subjectGraph");
    inline_field_values.add(subjectGraph.getName());
    inline_field_names.add("skeletonGraph");
    inline_field_values.add(skeletonGraph.getName());
  }
}
