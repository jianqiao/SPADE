package spade.storage;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.codec.digest.DigestUtils;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import spade.core.AbstractEdge;
import spade.core.AbstractStorage;
import spade.core.AbstractVertex;

public class PostgreSQL extends AbstractStorage {
  // Bulk insert batch size
  private static final int MAX_VERTEX_BATCH_SIZE = 1024 * 8;
  private static final int MAX_EDGE_BATCH_SIZE = MAX_VERTEX_BATCH_SIZE * 2;

  private static class VertexAndEdgeBatch {
    public ArrayList<AbstractVertex> vertices = new ArrayList<AbstractVertex>();
    public ArrayList<AbstractEdge> edges = new ArrayList<AbstractEdge>();
    public void clear() {
      vertices.clear();
      edges.clear();
    }
  }

  private static class PostgresCopyHelper extends Thread {
    private long timeExecutionStart;
    private long verticesCommitted = 0;
    private long edgesCommitted = 0;
    private PrintWriter processIndicatorWriter = null;

    private static class CopyExecutor extends Thread {
      private Connection connection = null;
      private CopyManager copyManager;

      private String copyCommand;
      private String data;
      private Semaphore producer = new Semaphore(1);
      private Semaphore consumer = new Semaphore(0);

      public CopyExecutor(String url, String copyCommand) throws ClassNotFoundException, SQLException {
        Class.forName("org.postgresql.Driver");
        connection = DriverManager.getConnection(url);
        copyManager = new CopyManager((BaseConnection) connection);
        this.copyCommand = copyCommand;
      }

      public void commit(String data) throws InterruptedException {
        producer.acquire();
        this.data = data;
        consumer.release();
      }

      public void shutDown() {
        this.interrupt();
        try {
          connection.close();
        } catch (SQLException ex) {
          Logger.getLogger(PostgreSQL.class.getName()).log(Level.SEVERE, null, ex);
        }
      }

      @Override
      public void run() {
        try {
          while (true) {
            try {
              consumer.acquire();
              copyManager.copyIn(copyCommand, new StringReader(data));
              data = null;
            } catch (SQLException | IOException ex) {
              Logger.getLogger(PostgreSQL.class.getName()).log(Level.SEVERE, null, ex);
            }
            producer.release();
          }
        } catch (InterruptedException ex) {
        }
      }
    }

    // Postgresql jdbc connection
    private Connection connection = null;
    private CopyManager copyManager;

    // Double buffer, simple producer-consumer pattern
    private VertexAndEdgeBatch batchBuffer = new VertexAndEdgeBatch();
    private boolean batchReady = false;
    private int[][] edgeIdPairs = new int[MAX_EDGE_BATCH_SIZE][2];

    private StringBuilder vertexSha256 = new StringBuilder();
    private StringBuilder vertexAnnos = new StringBuilder();
    private StringBuilder edgeSha256 = new StringBuilder();
    private StringBuilder edges = new StringBuilder();
    private StringBuilder edgeAnnos = new StringBuilder();

    private Semaphore producer = new Semaphore(1);
    private Semaphore consumer = new Semaphore(0);

    private CopyExecutor vertexAnnoCopyExecutor;
    private CopyExecutor edgeAnnoCopyExecutor;

    public boolean initialize(String url) throws ClassNotFoundException, SQLException {
      Class.forName("org.postgresql.Driver");
      connection = DriverManager.getConnection(url);
      copyManager = new CopyManager((BaseConnection) connection);
      vertexAnnoCopyExecutor =
          new CopyExecutor(url, "COPY vertex_anno FROM STDIN WITH DELIMITER '|'");
      vertexAnnoCopyExecutor.start();
      edgeAnnoCopyExecutor =
          new CopyExecutor(url, "COPY edge_anno FROM STDIN WITH DELIMITER '|'");
      edgeAnnoCopyExecutor.start();
      this.start();
      return true;
    }

    public boolean shutDown() {
      if (processIndicatorWriter != null) {
        processIndicatorWriter.close();
      }
      try {
        connection.close();
      } catch (SQLException ex) {
        Logger.getLogger(PostgreSQL.class.getName()).log(Level.SEVERE, null, ex);
      }
      vertexAnnoCopyExecutor.shutDown();;
      edgeAnnoCopyExecutor.shutDown();;
      this.interrupt();
      return true;
    }

    public VertexAndEdgeBatch commit(VertexAndEdgeBatch batch) throws InterruptedException {
      producer.acquire();
      VertexAndEdgeBatch ret = batchBuffer;
      batchBuffer = batch;
      batchReady = true;
      consumer.release();
      return ret;
    }

    @Override
    public void run() {
      timeExecutionStart = System.currentTimeMillis();
      try {
        while (true) {
          consumer.acquire();
          if (batchReady) {
            try {
              processBatchInner();
            } catch (SQLException | IOException ex) {
              Logger.getLogger(PostgreSQL.class.getName()).log(Level.SEVERE, null, ex);
            }
            batchBuffer.clear();
            batchReady = false;
          }
          producer.release();
        }
      } catch (InterruptedException ex) {
        Logger.getLogger(PostgreSQL.class.getName()).log(Level.SEVERE, null, ex);
      }
    }

    private void processBatchInner() throws SQLException, IOException, InterruptedException {
      ArrayList<AbstractVertex> vertexBatchBuffer = batchBuffer.vertices;
      ArrayList<AbstractEdge> edgeBatchBuffer = batchBuffer.edges;
      Statement stmt = connection.createStatement();

      if (vertexBatchBuffer.size() > 0) {
        vertexSha256.setLength(0);
        for (AbstractVertex v : vertexBatchBuffer) {
          vertexSha256.append(DigestUtils.sha256Hex(v.toString()));
          vertexSha256.append('\n');
        }
        copyManager.copyIn(
            "COPY vertex(sha256) FROM STDIN",
            new StringReader(vertexSha256.toString()));

        ResultSet rs = stmt.executeQuery("SELECT last_value FROM vertex_id_seq");
        rs.next();
        long start_id = rs.getLong(1) - vertexBatchBuffer.size() + 1;
        rs.close();

        vertexAnnos.setLength(0);
        for (int i = 0; i < vertexBatchBuffer.size(); ++i) {
          String id = String.valueOf(start_id + i);
          for (Map.Entry<String, String> annoEntry :
                   vertexBatchBuffer.get(i).getAnnotations().entrySet()) {
            String annoKey = annoEntry.getKey();
            if (!annoKey.startsWith("_$")) {
              vertexAnnos.append(id);
              vertexAnnos.append('|');
              vertexAnnos.append(annoKey);
              vertexAnnos.append('|');
              AppendEscaped(vertexAnnos, annoEntry.getValue());
              vertexAnnos.append('\n');
            }
          }
        }
        vertexAnnoCopyExecutor.commit(vertexAnnos.toString());
      }

      if (edgeBatchBuffer.size() > 0) {
        edgeSha256.setLength(0);
        for (int i = 0; i < edgeBatchBuffer.size(); ++i) {
          edgeSha256.append(i);
          edgeSha256.append('|');
          edgeSha256.append(DigestUtils.sha256Hex(
              edgeBatchBuffer.get(i).getSourceVertex().toString()));
          edgeSha256.append('|');
          edgeSha256.append(DigestUtils.sha256Hex(
              edgeBatchBuffer.get(i).getDestinationVertex().toString()));
          edgeSha256.append('\n');
        }
        stmt.execute("TRUNCATE TABLE edge_sha256_buffer;");
        copyManager.copyIn(
            "COPY edge_sha256_buffer FROM STDIN WITH DELIMITER '|'",
            new StringReader(edgeSha256.toString()));

        ResultSet rs = stmt.executeQuery(
            "SELECT b.id, s.id, d.id " +
            "FROM   vertex s, vertex d, edge_sha256_buffer b " +
            "WHERE  s.sha256 = b.src AND d.sha256 = b.dst");
        while (rs.next()) {
          int rowId = rs.getInt(1);
          edgeIdPairs[rowId][0] = rs.getInt(2);
          edgeIdPairs[rowId][1] = rs.getInt(3);
        }
        rs.close();

        edges.setLength(0);
        for (int i = 0; i < edgeBatchBuffer.size(); ++i) {
          edges.append(edgeIdPairs[i][0]);
          edges.append('|');
          edges.append(edgeIdPairs[i][1]);
          edges.append('\n');
        }
        copyManager.copyIn(
            "COPY edge(src, dst) FROM STDIN WITH DELIMITER '|'",
            new StringReader(edges.toString()));

        rs = stmt.executeQuery("SELECT last_value FROM edge_id_seq");
        rs.next();
        long start_id = rs.getLong(1) - edgeBatchBuffer.size() + 1;
        rs.close();

        edgeAnnos.setLength(0);
        for (int i = 0; i < edgeBatchBuffer.size(); ++i) {
          String id = String.valueOf(start_id + i);
          for (Map.Entry<String, String> annoEntry :
                   edgeBatchBuffer.get(i).getAnnotations().entrySet()) {
            String annoKey = annoEntry.getKey();
            if (!annoKey.startsWith("_$")) {
              edgeAnnos.append(id);
              edgeAnnos.append('|');
              edgeAnnos.append(annoEntry.getKey());
              edgeAnnos.append('|');
              AppendEscaped(edgeAnnos, annoEntry.getValue());
              edgeAnnos.append('\n');
            }
          }
        }
        edgeAnnoCopyExecutor.commit(edgeAnnos.toString());
      }

      stmt.close();

      verticesCommitted += vertexBatchBuffer.size();
      edgesCommitted += edgeBatchBuffer.size();
      long timeElapsed = System.currentTimeMillis() - timeExecutionStart;
      if (processIndicatorWriter != null) {
        processIndicatorWriter.println(
            "Time: " + (timeElapsed / 1000) +
            " Vertices: " + verticesCommitted +
            " Edges: " + edgesCommitted);
        processIndicatorWriter.flush();
      }
    }

    private void AppendEscaped(StringBuilder sb, String str) {
      for (int i = 0; i < str.length(); ++i) {
        char c = str.charAt(i);
        switch (c) {
          case '\\':
            sb.append("\\\\");
            break;
          case '|':
            sb.append("\\|");
            break;
          default:
            sb.append(c);
        }
      }
    }
  }

  private VertexAndEdgeBatch batch = new VertexAndEdgeBatch();
  private PostgresCopyHelper pgCopyHelper;
  private Connection pgConnection;

  private HashMap<String, String> parseKeyValuePairs(String arguments) {
    HashMap<String, String> ret = new HashMap<String, String>();
    Pattern keyValuePairPattern = Pattern.compile("[a-zA-Z]*\\s*=\\s*(\\\\ |[^ ])*");
    Matcher m = keyValuePairPattern.matcher(arguments);
    while (m.find()) {
      String[] pair = m.group().split("=", 2);
      if (pair.length == 2) {
        String key = pair[0].trim();
        String value = (pair[1] + ".").trim().replace("\\ ", " ");
        value = value.substring(0, value.length()-1);
        ret.put(key, value);
      }
    }
    return ret;
  }

  @Override
  public boolean initialize(String arguments) {
    HashMap<String, String> argMap = parseKeyValuePairs(arguments);

    String[] requiredArgKeys = new String[] {
        "jdbcUrl"
    };
    for (String key : requiredArgKeys) {
      if (!argMap.containsKey(key)) {
        Logger.getLogger(PostgreSQL.class.getName()).log(
            Level.SEVERE, "Parameter required: " + key);
        return false;
      }
    }

    try {
      Class.forName("org.postgresql.Driver");
      pgConnection = DriverManager.getConnection(argMap.get("jdbcUrl"));
      pgCopyHelper = new PostgresCopyHelper();
      pgCopyHelper.initialize(argMap.get("jdbcUrl"));
    } catch (ClassNotFoundException | SQLException |
             NullPointerException | NumberFormatException ex) {
      Logger.getLogger(PostgreSQL.class.getName()).log(Level.SEVERE, null, ex);
      return false;
    }

    return true;
  }

  @Override
  public boolean shutdown() {
    commitBatch();
    try {
      pgConnection.close();
    } catch (SQLException ex) {
      Logger.getLogger(PostgreSQL.class.getName()).log(Level.SEVERE, null, ex);
    }
    pgCopyHelper.shutDown();
    return true;
  }

  @Override
  public boolean putVertex(AbstractVertex incomingVertex) {
    batch.vertices.add(incomingVertex);
    if (batch.vertices.size() >= MAX_VERTEX_BATCH_SIZE) {
      commitBatch();
    }
    return true;
  }

  @Override
  public boolean putEdge(AbstractEdge incomingEdge) {
    batch.edges.add(incomingEdge);
    if (batch.edges.size() >= MAX_EDGE_BATCH_SIZE) {
      commitBatch();
    }
    return true;
  }

  private void commitBatch() {
    try {
      batch = pgCopyHelper.commit(batch);
    } catch (InterruptedException ex) {
      Logger.getLogger(PostgreSQL.class.getName()).log(Level.SEVERE, null, ex);
    }
  }
}
