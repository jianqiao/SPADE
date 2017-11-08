package spade.query.quickgrail.execution;

import java.util.ArrayList;

import spade.query.quickgrail.entities.Graph;
import spade.query.quickgrail.kernel.Environment;
import spade.query.quickgrail.utility.TreeStringSerializable;
import spade.storage.Quickstep;

public class GetShortestPath extends Instruction {
  private Graph targetGraph;
  private Graph subjectGraph;
  private Graph srcGraph;
  private Graph dstGraph;
  private Integer maxDepth;

  public GetShortestPath(Graph targetGraph, Graph subjectGraph,
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

    String filter;
    qs.execute("DROP TABLE m_conn;\n" +
               "CREATE TABLE m_conn (src INT, dst INT);");
    if (Environment.IsBaseGraph(subjectGraph)) {
      filter = "";
      qs.execute("\\analyzecount edge\n" +
                 "INSERT INTO m_conn SELECT src, dst FROM edge GROUP BY src, dst;");
    } else {
      filter = " AND edge.id IN (SELECT id FROM " + subjectGraph.getEdgeTableName() + ")";
      qs.execute("DROP TABLE m_sgedge;\n" +
                 "CREATE TABLE m_sgedge (src INT, dst INT);\n" +
                 "INSERT INTO m_sgedge SELECT src, dst FROM edge" +
                 " WHERE id IN (SELECT id FROM " + subjectGraph.getEdgeTableName() + ");\n" +
                 "\\analyzecount m_sgedge\n" +
                 "INSERT INTO m_conn SELECT src, dst FROM m_sgedge GROUP BY src, dst;\n" +
                 "DROP TABLE m_sgedge;");
    }
    qs.execute("\\analyze m_conn\n");

    // Create subgraph edges table.
    qs.execute("DROP TABLE m_sgconn;\n" +
               "CREATE TABLE m_sgconn (src INT, dst INT, reaching INT, depth INT);");

    qs.execute("DROP TABLE m_cur;\n" +
               "DROP TABLE m_next;\n" +
               "DROP TABLE m_answer;\n" +
               "CREATE TABLE m_cur (id INT, reaching INT);\n" +
               "CREATE TABLE m_next (id INT, reaching INT);\n" +
               "CREATE TABLE m_answer (id INT);");

    qs.execute("INSERT INTO m_cur SELECT id, id FROM " + dstGraph.getVertexTableName() + ";\n" +
               "\\analyzerange m_cur\n" +
               "INSERT INTO m_answer SELECT id FROM m_cur GROUP BY id;");

    String loopStmts =
        "\\analyzecount m_cur\n" +
        "INSERT INTO m_sgconn SELECT src, dst, reaching, $depth" +
        " FROM m_cur, m_conn WHERE id = dst;\n" +
        "DROP TABLE m_next;\n" + "CREATE TABLE m_next (id INT, reaching INT);\n" +
        "INSERT INTO m_next SELECT src, reaching" +
        " FROM m_cur, m_conn WHERE id = dst;\n" +
        "DROP TABLE m_cur;\n" + "CREATE TABLE m_cur (id INT, reaching INT);\n" +
        "\\analyzerange m_answer\n" +
        "\\analyzecount m_next\n" +
        "INSERT INTO m_cur SELECT id, reaching FROM m_next" +
        " WHERE id NOT IN (SELECT id FROM m_answer) GROUP BY id, reaching;\n" +
        "\\analyzerange m_cur\n" +
        "INSERT INTO m_answer SELECT id FROM m_cur GROUP BY id;";
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
               "INSERT INTO m_answer SELECT id FROM m_cur;");

    qs.execute("\\analyzerange m_answer\n" +
               "\\analyze m_sgconn\n");

    loopStmts =
        "DROP TABLE m_next;\n" + "CREATE TABLE m_next(id INT);\n" +
        "INSERT INTO m_next SELECT MIN(dst)" +
        " FROM m_cur, m_sgconn WHERE id = src" +
        " AND depth + $depth <= " + String.valueOf(maxDepth) + " GROUP BY src, reaching;\n" +
        "DROP TABLE m_cur;\n" + "CREATE TABLE m_cur(id INT);\n" +
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
               "DROP TABLE m_conn;\n" +
               "DROP TABLE m_sgconn;");
  }

  @Override
  public String getLabel() {
    return "GetShortestPath";
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
