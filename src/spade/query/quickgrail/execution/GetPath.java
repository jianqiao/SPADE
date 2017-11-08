package spade.query.quickgrail.execution;

import java.util.ArrayList;

import spade.query.quickgrail.entities.Graph;
import spade.query.quickgrail.kernel.Environment;
import spade.query.quickgrail.utility.TreeStringSerializable;
import spade.storage.Quickstep;

public class GetPath extends Instruction {
  private Graph targetGraph;
  private Graph subjectGraph;
  private Graph srcGraph;
  private Graph dstGraph;
  private Integer maxDepth;

  public GetPath(Graph targetGraph, Graph subjectGraph,
                 Graph srcGraph, Graph dstGraph,
                 Integer maxDepth) {
    this.targetGraph = targetGraph;
    this.subjectGraph = subjectGraph;
    this.srcGraph = srcGraph;
    this.dstGraph = dstGraph;
    this.maxDepth = maxDepth;
  }

  @Override
  public void execute(ExecutionContext ctx) {
    Quickstep qs = ctx.getQuickstepInstance();

    qs.execute("DROP TABLE m_cur;\n" +
               "DROP TABLE m_next;\n" +
               "DROP TABLE m_answer;\n" +
               "CREATE TABLE m_cur (id INT);\n" +
               "CREATE TABLE m_next (id INT);\n" +
               "CREATE TABLE m_answer (id INT);");

    String filter;
    if (Environment.IsBaseGraph(subjectGraph)) {
      filter = "";
    } else {
      filter = " AND edge.id IN (SELECT id FROM " + subjectGraph.getEdgeTableName() + ")";
    }

    // Create subgraph edges table.
    qs.execute("DROP TABLE m_sgconn;\n" +
               "CREATE TABLE m_sgconn (src INT, dst INT, depth INT);");

    qs.execute("INSERT INTO m_cur SELECT id FROM " + dstGraph.getVertexTableName() + ";\n" +
               "INSERT INTO m_answer SELECT id FROM m_cur;\n" +
               "\\analyzerange edge\n");

    String loopStmts =
        "\\analyzerange m_cur\n" +
        "INSERT INTO m_sgconn SELECT src, dst, $depth FROM edge" +
        " WHERE dst IN (SELECT id FROM m_cur)" + filter + ";\n" +
        "DROP TABLE m_next;\n" + "CREATE TABLE m_next (id INT);\n" +
        "INSERT INTO m_next SELECT src FROM edge" +
        " WHERE dst IN (SELECT id FROM m_cur)" + filter + " GROUP BY src;\n" +
        "DROP TABLE m_cur;\n" + "CREATE TABLE m_cur (id INT);\n" +
        "\\analyzerange m_answer\n" +
        "INSERT INTO m_cur SELECT id FROM m_next WHERE id NOT IN (SELECT id FROM m_answer);\n" +
        "INSERT INTO m_answer SELECT id FROM m_cur;";
    for (int i = 0; i < maxDepth; ++i) {
      qs.execute(loopStmts.replace("$depth", String.valueOf(i+1)));

      String worksetSizeQuery = "COPY SELECT COUNT(*) FROM m_cur TO stdout;";
      if (qs.executeForLongResult(worksetSizeQuery) == 0) {
        break;
      }
    }

    qs.execute("DROP TABLE m_cur;\n" +
               "DROP TABLE m_next;\n" +
               "CREATE TABLE m_cur (id INT);\n" +
               "CREATE TABLE m_next (id INT);");

    qs.execute("\\analyzerange m_answer\n" +
               "INSERT INTO m_cur SELECT id FROM " + srcGraph.getVertexTableName() +
               " WHERE id IN (SELECT id FROM m_answer);\n");

    qs.execute("DROP TABLE m_answer;\n" +
               "CREATE TABLE m_answer (id INT);\n" +
               "INSERT INTO m_answer SELECT id FROM m_cur;" +
               "\\analyzerange m_answer m_sgconn\n");

    loopStmts =
        "DROP TABLE m_next;\n" + "CREATE TABLE m_next (id int);\n" +
        "\\analyzerange m_cur\n" +
        "INSERT INTO m_next SELECT dst FROM m_sgconn" +
        " WHERE src IN (SELECT id FROM m_cur)" +
        " AND depth + $depth <= " + String.valueOf(maxDepth) + " GROUP BY dst;\n" +
        "DROP TABLE m_cur;\n" + "CREATE TABLE m_cur (id INT);\n" +
        "\\analyzerange m_answer\n" +
        "INSERT INTO m_cur SELECT id FROM m_next WHERE id NOT IN (SELECT id FROM m_answer);\n" +
        "INSERT INTO m_answer SELECT id FROM m_cur;";
    for (int i = 0; i < maxDepth; ++i) {
      qs.execute(loopStmts.replace("$depth", String.valueOf(i)));

      String worksetSizeQuery = "COPY SELECT COUNT(*) FROM m_cur TO stdout;";
      if (qs.executeForLongResult(worksetSizeQuery) == 0) {
        break;
      }
    }

    String targetVertexTable = targetGraph.getVertexTableName();
    String targetEdgeTable = targetGraph.getEdgeTableName();

    qs.execute("\\analyzerange m_answer\n" +
               "INSERT INTO " + targetVertexTable + " SELECT id FROM m_answer;\n" +
               "INSERT INTO " + targetEdgeTable + " SELECT id FROM edge" +
               " WHERE src IN (SELECT id FROM m_answer)" +
               " AND dst IN (SELECT id FROM m_answer)" + filter + ";");

    qs.execute("DROP TABLE m_cur;\n" +
               "DROP TABLE m_next;\n" +
               "DROP TABLE m_answer;\n" +
               "DROP TABLE m_sgconn;");
  }

  @Override
  public String getLabel() {
    return "GetPath";
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
    inline_field_names.add("srcGraph");
    inline_field_values.add(srcGraph.getName());
    inline_field_names.add("dstGraph");
    inline_field_values.add(dstGraph.getName());
    inline_field_names.add("maxDepth");
    inline_field_values.add(String.valueOf(maxDepth));
  }
}
