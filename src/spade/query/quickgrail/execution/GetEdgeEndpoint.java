package spade.query.quickgrail.execution;

import java.util.ArrayList;

import spade.query.quickgrail.entities.Graph;
import spade.query.quickgrail.utility.TreeStringSerializable;
import spade.storage.Quickstep;

public class GetEdgeEndpoint extends Instruction {
  public enum Component {
    kSource,
    kDestination,
    kBoth
  }

  private Graph targetGraph;
  private Graph subjectGraph;
  private Component component;

  public GetEdgeEndpoint(Graph targetGraph, Graph subjectGraph, Component component) {
    this.targetGraph = targetGraph;
    this.subjectGraph = subjectGraph;
    this.component = component;
  }

  @Override
  public void execute(ExecutionContext ctx) {
    Quickstep qs = ctx.getQuickstepInstance();

    String targetVertexTable = targetGraph.getVertexTableName();
    String subjectEdgeTable = subjectGraph.getEdgeTableName();

    qs.execute("DROP TABLE m_answer;\n" +
               "CREATE TABLE m_answer (id INT);\n" +
               "\\analyzerange " + subjectEdgeTable + "\n");

    if (component == Component.kSource || component == Component.kBoth) {
      qs.execute("INSERT INTO m_answer SELECT src FROM edge" +
                 " WHERE id IN (SELECT id FROM " + subjectEdgeTable + ");");
    }

    if (component == Component.kDestination || component == Component.kBoth) {
      qs.execute("INSERT INTO m_answer SELECT dst FROM edge" +
                 " WHERE id IN (SELECT id FROM " + subjectEdgeTable + ");");
    }

    qs.execute("\\analyzerange m_answer\n" +
               "INSERT INTO " + targetVertexTable + " SELECT id FROM m_answer GROUP BY id;");

    qs.execute("DROP TABLE m_answer;");
}

  @Override
  public String getLabel() {
    return "GetEdgeEndpoint";
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
    inline_field_names.add("component");
    inline_field_values.add(component.name().substring(1));
  }

}
