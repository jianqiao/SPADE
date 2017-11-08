package spade.query.quickgrail.execution;

import java.util.ArrayList;
import java.util.HashMap;

import spade.core.AbstractEdge;
import spade.core.AbstractVertex;
import spade.core.Edge;
import spade.core.Vertex;
import spade.query.quickgrail.entities.Graph;
import spade.query.quickgrail.utility.Response;
import spade.query.quickgrail.utility.TreeStringSerializable;
import spade.storage.Quickstep;

public class ExportGraph extends Instruction {
  private static final int kNonForceExportLimit = 1000;

  private Graph targetGraph;
  private Format format;
  private boolean force;
  private String targetPath;

  public enum Format {
    kNormal,
    kDot
  }

  public ExportGraph(Graph targetGraph, Format format,
                     boolean force, String targetPath) {
    this.targetGraph = targetGraph;
    this.format = format;
    this.force = force;
    this.targetPath = targetPath;
  }

  @Override
  public void execute(ExecutionContext ctx) {
    Quickstep qs = ctx.getQuickstepInstance();

    if (!force) {
      long numVertices = qs.executeForLongResult(
          "COPY SELECT COUNT(*) FROM " + targetGraph.getVertexTableName() + " TO stdout;");
      if (numVertices > kNonForceExportLimit) {
        String cmd = (format == Format.kNormal ? "dump" : "export");
        ctx.addResponse("It may take a long time to transfer/print the result data due to " +
                        "too many vertices: " + numVertices + "\n" +
                        "Please use *" + cmd + " force ...* to force the transfer");
        return;
      }
    }

    HashMap<Integer, AbstractVertex> vertices =
        exportVertices(qs, targetGraph.getVertexTableName());
    HashMap<Long, AbstractEdge> edges =
        exportEdges(qs, targetGraph.getEdgeTableName());

    spade.core.Graph graph = new spade.core.Graph();
    graph.vertexSet().addAll(vertices.values());
    graph.edgeSet().addAll(edges.values());

    if (format == Format.kNormal) {
      ctx.addResponse(graph);
    } else {
      Response response = new Response(graph);
      response.setMetaData("type", "Export");
      response.setMetaData("format", "dot");
      response.setMetaData("path", targetPath);
      ctx.addResponse(response);
    }
  }

  private HashMap<Integer, AbstractVertex> exportVertices(
      Quickstep qs, String targetVertexTable) {
    HashMap<Integer, AbstractVertex> vertices = new HashMap<Integer, AbstractVertex>();
    long numVertices = qs.executeForLongResult(
        "COPY SELECT COUNT(*) FROM " + targetVertexTable + " TO stdout;");
    if (numVertices == 0) {
      return vertices;
    }

    qs.execute("\\analyzerange " + targetVertexTable + "\n");

    String vertexAnnoStr = qs.execute(
        "COPY SELECT * FROM vertex_anno WHERE id IN (SELECT id FROM " +
        targetVertexTable + ") TO stdout WITH (DELIMITER e'\\n');");
    String[] vertexAnnoLines = vertexAnnoStr.split("\n");
    vertexAnnoStr = null;

    assert vertexAnnoLines.length % 3 == 0;
    for (int i = 0; i < vertexAnnoLines.length; i += 3) {
      // TODO: accelerate with cache.
      Integer id = Integer.parseInt(vertexAnnoLines[i]);
      AbstractVertex vertex = vertices.get(id);
      if (vertex == null) {
        vertex = new Vertex();
        vertices.put(id, vertex);
      }
      vertex.addAnnotation(vertexAnnoLines[i+1], vertexAnnoLines[i+2]);
    }
    return vertices;
  }

  private HashMap<Long, AbstractEdge> exportEdges(
      Quickstep qs, String targetEdgeTable) {
    HashMap<Long, AbstractEdge> edges = new HashMap<Long, AbstractEdge>();

    long numEdges = qs.executeForLongResult(
        "COPY SELECT COUNT(*) FROM " + targetEdgeTable + " TO stdout;");
    if (numEdges == 0) {
      return edges;
    }

    qs.execute("DROP TABLE m_answer;\n" +
               "CREATE TABLE m_answer(id INT);\n" +
               "DROP TABLE m_answer_edge;\n" +
               "CREATE TABLE m_answer_edge(id LONG, src INT, dst INT);\n" +
               "\\analyzerange " + targetEdgeTable + "\n" +
               "INSERT INTO m_answer_edge SELECT * FROM edge" +
               " WHERE id IN (SELECT id FROM " + targetEdgeTable + ");\n" +
               "INSERT INTO m_answer SELECT src FROM m_answer_edge;\n" +
               "INSERT INTO m_answer SELECT dst FROM m_answer_edge;");

    HashMap<Integer, AbstractVertex> vertices = exportVertices(qs, "m_answer");

    String edgeStr = qs.execute(
        "COPY SELECT * FROM m_answer_edge TO stdout WITH (DELIMITER e'\\n');");
    String[] edgeLines = edgeStr.split("\n");
    edgeStr = null;

    assert edgeLines.length % 3 == 0;
    for (int i = 0; i < edgeLines.length; i += 3) {
      Long id = Long.parseLong(edgeLines[i]);
      Integer src = Integer.parseInt(edgeLines[i+1]);
      Integer dst = Integer.parseInt(edgeLines[i+2]);
      edges.put(id, new Edge(vertices.get(src), vertices.get(dst)));
    }
    edgeLines = null;

    String edgeAnnoStr = qs.execute(
        "COPY SELECT * FROM edge_anno WHERE id IN (SELECT id FROM " +
        targetEdgeTable + ") TO stdout WITH (DELIMITER e'\\n');");
    String[] edgeAnnoLines = edgeAnnoStr.split("\n");
    edgeAnnoStr = null;

    assert edgeAnnoLines.length % 3 == 0;
    for (int i = 0; i < edgeAnnoLines.length; i += 3) {
      // TODO: accelerate with cache.
      Long id = Long.parseLong(edgeAnnoLines[i]);
      AbstractEdge edge = edges.get(id);
      if (edge == null) {
        continue;
      }
      edge.addAnnotation(edgeAnnoLines[i+1], edgeAnnoLines[i+2]);
    }
    qs.execute("DROP TABLE m_answer;\n" +
               "DROP TABLE m_answer_edge;");
    return edges;
  }

  @Override
  public String getLabel() {
    return "ExportGraph";
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
