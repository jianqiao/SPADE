package spade.query.quickgrail.utility;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import spade.query.quickgrail.entities.Graph;
import spade.query.quickgrail.entities.GraphMetadata;
import spade.storage.Quickstep;

public class QuickstepUtil {
  private static Pattern tableNamePattern = Pattern.compile("([^ \n]+)[ |].*table.*");

  public static void CreateEmptyGraph(Quickstep qs, Graph graph) {
    String vertexTable = graph.getVertexTableName();
    String edgeTable = graph.getEdgeTableName();

    StringBuilder sb = new StringBuilder();
    sb.append("DROP TABLE " + vertexTable + ";\n");
    sb.append("DROP TABLE " + edgeTable + ";\n");
    sb.append("CREATE TABLE " + vertexTable + " (id INT) " +
              "WITH BLOCKPROPERTIES (TYPE columnstore, SORT id, BLOCKSIZEMB 4);\n");
    sb.append("CREATE TABLE " + edgeTable + " (id LONG) " +
              "WITH BLOCKPROPERTIES (TYPE columnstore, SORT id, BLOCKSIZEMB 4);\n");
    qs.execute(sb.toString());
  }

  public static void CreateEmptyGraphMetadata(Quickstep qs, GraphMetadata metadata) {
    String vertexTable = metadata.getVertexTableName();
    String edgeTable = metadata.getEdgeTableName();

    StringBuilder sb = new StringBuilder();
    sb.append("DROP TABLE " + vertexTable + ";\n");
    sb.append("DROP TABLE " + edgeTable + ";\n");
    sb.append("CREATE TABLE " + vertexTable + " (id INT, name VARCHAR(64), value VARCHAR(256));");
    sb.append("CREATE TABLE " + edgeTable + " (id LONG, name VARCHAR(64), value VARCHAR(256));");
    qs.execute(sb.toString());
  }

  public static ArrayList<String> GetAllTableNames(Quickstep qs) {
    ArrayList<String> tableNames = new ArrayList<String>();
    String output = qs.execute("\\d\n");
    Matcher matcher = tableNamePattern.matcher(output);
    while (matcher.find()) {
      tableNames.add(matcher.group(1));
    }
    return tableNames;
  }
}
