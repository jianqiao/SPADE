package spade.query.quickgrail.execution;

import java.util.ArrayList;

import spade.query.quickgrail.entities.Graph;
import spade.query.quickgrail.kernel.Environment;
import spade.query.quickgrail.utility.TreeStringSerializable;
import spade.storage.Quickstep;

public class GetLineage extends Instruction {
  public enum Direction {
    kAncestor,
    kDescendant,
    kBoth
  }

  private Graph targetGraph;
  private Graph subjectGraph;
  private Graph startGraph;
  private Integer depth;
  private Direction direction;

  public GetLineage(Graph targetGraph, Graph subjectGraph,
                    Graph startGraph, Integer depth, Direction direction) {
    this.targetGraph = targetGraph;
    this.subjectGraph = subjectGraph;
    this.startGraph = startGraph;
    this.depth = depth;
    this.direction = direction;
  }

  @Override
  public void execute(ExecutionContext ctx) {
    ArrayList<Direction> oneDirs = new ArrayList<Direction>();
    if (direction == Direction.kBoth) {
      oneDirs.add(Direction.kAncestor);
      oneDirs.add(Direction.kDescendant);
    } else {
      oneDirs.add(direction);
    }

    Quickstep qs = ctx.getQuickstepInstance();

    String targetVertexTable = targetGraph.getVertexTableName();
    String targetEdgeTable = targetGraph.getEdgeTableName();

    String subjectEdgeTable = subjectGraph.getEdgeTableName();
    String filter = "";
    if (!Environment.IsBaseGraph(subjectGraph)) {
      qs.execute("\\analyzerange " + subjectEdgeTable + "\n");
      filter = " AND edge.id IN (SELECT id FROM " + subjectEdgeTable + ")";
    }

    for (Direction oneDir : oneDirs) {
      executeOneDirection(oneDir, qs, filter);
      qs.execute("\\analyzerange m_answer m_answer_edge\n" +
                 "INSERT INTO " + targetVertexTable + " SELECT id FROM m_answer;\n" +
                 "INSERT INTO " + targetEdgeTable + " SELECT id FROM m_answer_edge GROUP BY id;");
    }

    qs.execute("DROP TABLE m_cur;\n" +
               "DROP TABLE m_next;\n" +
               "DROP TABLE m_answer;\n" +
               "DROP TABLE m_answer_edge;");
  }

  private void executeOneDirection(Direction dir, Quickstep qs, String filter) {
    String src, dst;
    if (dir == Direction.kAncestor) {
      src = "dst";
      dst = "src";
    } else {
      assert dir == Direction.kDescendant;
      src = "src";
      dst = "dst";
    }

    qs.execute("DROP TABLE m_cur;\n" +
               "DROP TABLE m_next;\n" +
               "DROP TABLE m_answer;\n" +
               "DROP TABLE m_answer_edge;\n" +
               "CREATE TABLE m_cur (id INT);\n" +
               "CREATE TABLE m_next (id INT);\n" +
               "CREATE TABLE m_answer (id INT);\n" +
               "CREATE TABLE m_answer_edge (id LONG);");

    String startVertexTable = startGraph.getVertexTableName();
    qs.execute("INSERT INTO m_cur SELECT id FROM " + startVertexTable + ";\n" +
               "INSERT INTO m_answer SELECT id FROM m_cur;");

    String loopStmts =
        "DROP TABLE m_next;\n" + "CREATE TABLE m_next (id INT);\n" +
        "\\analyzerange m_cur\n" +
        "INSERT INTO m_next SELECT " + dst + " FROM edge" +
        " WHERE " + src + " IN (SELECT id FROM m_cur)" + filter +
        " GROUP BY " + dst + ";\n" +
        "INSERT INTO m_answer_edge SELECT id FROM edge" +
        " WHERE " + src + " IN (SELECT id FROM m_cur)" + filter + ";\n" +
        "DROP TABLE m_cur;\n" + "CREATE TABLE m_cur (id INT);\n" +
        "\\analyzerange m_answer\n" +
        "INSERT INTO m_cur SELECT id FROM m_next WHERE id NOT IN (SELECT id FROM m_answer);\n" +
        "INSERT INTO m_answer SELECT id FROM m_cur;";
    for (int i = 0; i < depth; ++i) {
      qs.execute(loopStmts);

      String worksetSizeQuery = "COPY SELECT COUNT(*) FROM m_cur TO stdout;";
      if (qs.executeForLongResult(worksetSizeQuery) == 0) {
        break;
      }
    }
  }

  @Override
  public String getLabel() {
    return "GetLineage";
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
    inline_field_names.add("startGraph");
    inline_field_values.add(startGraph.getName());
    inline_field_names.add("depth");
    inline_field_values.add(String.valueOf(depth));
    inline_field_names.add("direction");
    inline_field_values.add(direction.name().substring(1));
  }
}
