package spade.storage;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import spade.core.AbstractEdge;
import spade.core.AbstractStorage;
import spade.core.AbstractVertex;
import spade.core.Graph;

public class Quickstep extends AbstractStorage {
  // Bulk insert batch size
  private static final int DEFAULT_EDGE_BATCH_SIZE = 1024 * 256;
  private static final int MINIMUM_EDGE_BATCH_SIZE = 1024 * 16;

  // Measured in seconds.
  private static final int DEFAULT_FORCE_SUBMIT_TIME_INTERVAL = 180;
  private static final int MINIMUM_FORCE_SUBMIT_TIME_INTERVAL = 30;

  private String clientPath;
  private String serverIP;
  private String serverPort;
  private String infoPath;
  private int edgeBatchSize = DEFAULT_EDGE_BATCH_SIZE;

  private PrintWriter infoWriter = null;
  private long timeExecutionStart;

  private Logger logger = Logger.getLogger("Quickstep");

  public void logInfo(String info) {
    if (infoWriter != null) {
      infoWriter.println(info);
      infoWriter.flush();
    } else {
      logger.log(Level.INFO, info);
    }
  }

  private static class GraphBatch {
    private static long batchIDCounter = 0;
    private long batchID = 0;
    private ArrayList<AbstractVertex> vertices = new ArrayList<AbstractVertex>();
    private ArrayList<AbstractEdge> edges = new ArrayList<AbstractEdge>();

    public GraphBatch() {
      batchID = ++batchIDCounter;
    }

    public void reset() {
      batchID = ++batchIDCounter;
      vertices.clear();
      edges.clear();
    }

    public long getBatchID() {
      return batchID;
    }

    public boolean isEmpty() {
      return vertices.isEmpty() && edges.isEmpty();
    }

    public void addVertex(AbstractVertex vertex) {
      vertices.add(vertex);
    }

    public void addEdge(AbstractEdge edge) {
      edges.add(edge);
    }

    public ArrayList<AbstractVertex> getVertices() {
      return vertices;
    }

    public ArrayList<AbstractEdge> getEdges() {
      return edges;
    }
  }

  private class QsExecutor implements Callable<Void> {

    private class QsQuery implements Callable<String> {
      private String query;

      public QsQuery(String query) {
        this.query = query;
      }

      @Override
      public String call() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append(clientPath);
        if (serverIP != null) {
          sb.append(" -cli_network_ip=" + serverIP);
        }
        if (serverPort != null) {
          sb.append(" -cli_network_port=" + serverPort);
        }

        if (query.length() < 256) {
          logInfo("[Quickstep query]\n" + query);
        } else {
          logInfo("[Quickstep query]\n" + query.substring(0, 64).replace("\n", "\\n") + " ...");
        }

        try {
          long queryStartTime = System.currentTimeMillis();
          Process p = Runtime.getRuntime().exec(sb.toString());
          p.getOutputStream().write(query.getBytes(Charset.forName("UTF-8")));
          p.getOutputStream().close();
          String stdout = getStringFromInputStream(p.getInputStream()).trim();
          p.waitFor();
          if (!stdout.isEmpty()) {
            if (stdout.length() < 64) {
              logInfo("[Quickstep output] " + stdout.replace("\n", "\\n"));
            } else {
              logInfo("[Quickstep output] " + stdout.substring(0, 64).replace("\n", "\\n") + " ...");
            }
          }
          logInfo("[Done] " + (System.currentTimeMillis() - queryStartTime) + "ms");
          return stdout;
        } catch (IOException | InterruptedException e) {
          logger.log(Level.SEVERE, e.getMessage());
        }
        return null;
      }

      private String getStringFromInputStream(InputStream stream) throws IOException {
        StringBuilder sb = new StringBuilder();
        BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
        String line = "";
        while ((line = reader.readLine())!= null) {
          sb.append(line);
          sb.append('\n');
        }
        return sb.toString();
      }
    }

    // Double buffer, simple producer-consumer pattern.
    private GraphBatch batchBuffer = new GraphBatch();
    private int[][] edgeIdPairs;

    private StringBuilder vertexMD5 = new StringBuilder();
    private StringBuilder vertexAnnos = new StringBuilder();
    private StringBuilder edgeMD5 = new StringBuilder();
    private StringBuilder edgeLinks = new StringBuilder();
    private StringBuilder edgeAnnos = new StringBuilder();

    private ExecutorService batchExecutor;
    private Future<Void> batchFuture;

    private ExecutorService queryExecutor;
    private Future<String> queryFuture;

    private Lock transactionLock;

    private Pattern tableNamePattern = Pattern.compile("([^ \n]+)[ |].*table.*");

    public void initialize() {
      edgeIdPairs = new int[edgeBatchSize][2];
      batchExecutor = Executors.newSingleThreadExecutor();
      queryExecutor = Executors.newSingleThreadExecutor();
      transactionLock = new ReentrantLock();
    }

    public void shutdown() {
      finalizeBatch();
      batchExecutor.shutdown();
      batchExecutor = null;
      finalizeQuery();
      queryExecutor.shutdown();
      queryExecutor = null;
    }

    public ArrayList<String> getAllTableNames() {
      ArrayList<String> tableNames = new ArrayList<String>();
      String output = executeQuery("\\d\n");
      Matcher matcher = tableNamePattern.matcher(output);
      while (matcher.find()) {
        tableNames.add(matcher.group(1));
      }
      return tableNames;
    }

    public void beginTransaction() {
      transactionLock.lock();
    }

    public void finalizeTransction() {
      // May need better handling of error recovery.
      transactionLock.unlock();
    }

    public void initStorage() {
      StringBuilder initQuery = new StringBuilder();
      initQuery.append("CREATE TABLE vertex (\n" +
                       "  id INT,\n" +
                       "  md5 CHAR(32)\n" +
                       ") WITH BLOCKPROPERTIES (\n" +
                       "  TYPE columnstore,\n" +
                       "  SORT id);");
      initQuery.append("CREATE TABLE edge (\n" +
                       "  id LONG,\n" +
                       "  src INT,\n" +
                       "  dst INT\n" +
                       ") WITH BLOCKPROPERTIES (\n" +
                       "  TYPE columnstore,\n" +
                       "  SORT id);");
      initQuery.append("CREATE TABLE vertex_anno (\n" +
                       "  id INT,\n" +
                       "  field VARCHAR(32),\n" +
                       "  value VARCHAR(49152)\n" +
                       ") WITH BLOCKPROPERTIES (\n" +
                       "  TYPE compressed_columnstore,\n" +
                       "  SORT id,\n" +
                       "  COMPRESS (id, field, value));");
      initQuery.append("CREATE TABLE edge_anno (\n" +
                       "  id LONG,\n" +
                       "  field VARCHAR(32),\n" +
                       "  value VARCHAR(256)\n" +
                       ") WITH BLOCKPROPERTIES (\n" +
                       "  TYPE compressed_columnstore,\n" +
                       "  SORT id,\n" +
                       "  COMPRESS (id, field, value));");
      initQuery.append("CREATE TABLE trace_base_vertex (\n" +
                       "  id INT\n" +
                       ") WITH BLOCKPROPERTIES (\n" +
                       "  TYPE columnstore, SORT id);");
      initQuery.append("CREATE TABLE trace_base_edge (\n" +
                       "  id LONG\n" +
                       ") WITH BLOCKPROPERTIES (\n" +
                       "  TYPE columnstore, SORT id);");
      executeQuery(initQuery.toString());
    }

    public void resetStorage() {
      ArrayList<String> allTables = getAllTableNames();
      if (!allTables.isEmpty()) {
        StringBuilder dropQuery = new StringBuilder();
        for (String table : allTables) {
          dropQuery.append("DROP TABLE " + table + ";\n");
        }
        executeQuery(dropQuery.toString());
      }
      initStorage();
    }

    public void resetStorageIfInvalid() {
      ArrayList<String> allTables = getAllTableNames();
      HashSet<String> tableSet = new HashSet<String>();
      for (String table : allTables) {
        tableSet.add(table);
      }
      String[] requiredTables = new String[] {
          "vertex",
          "vertex_anno",
          "edge",
          "edge_anno",
          "trace_base_vertex",
          "trace_base_edge",
      };

      boolean isInvalid = false;
      for (String table : requiredTables) {
        if (!tableSet.contains(table)) {
          isInvalid = true;
        }
      }
      if (isInvalid) {
        resetStorage();
      }
    }

    public GraphBatch submitBatch(GraphBatch batch) {
      logInfo("Submit batch " + batch.getBatchID() + " at " +
              formatTime(System.currentTimeMillis() - timeExecutionStart));
      finalizeBatch();
      GraphBatch ret = batchBuffer;
      batchBuffer = batch;
      batchFuture = batchExecutor.submit(this);
      return ret;
    }

    private void finalizeBatch() {
      if (batchFuture != null) {
        try {
          batchFuture.get();
        } catch (InterruptedException | ExecutionException e) {
          logger.log(Level.SEVERE, e.getMessage());
        }
        batchFuture = null;
      }
    }

    @Override
    public Void call() throws Exception {
      beginTransaction();
      processBatch();
      finalizeTransaction();
      batchBuffer.reset();
      return null;
    }

    private void processBatch() {
      ArrayList<AbstractVertex> vertices = batchBuffer.getVertices();
      ArrayList<AbstractEdge> edges = batchBuffer.getEdges();

      logInfo("Start processing batch " + batchBuffer.getBatchID() + " at " +
              formatTime(System.currentTimeMillis() - timeExecutionStart));

      if (vertices.size() > 0) {
        int lastNumVertices;
        try {
          String sn = executeQuery("COPY SELECT COUNT(*) FROM vertex TO stdout;");
          lastNumVertices = Integer.parseInt(sn);
        } catch (Exception e) {
          logger.log(Level.SEVERE, e.getMessage());
          return;
        }

        submitQuery("DROP TABLE vertex_md5_cache;\n" +
                    "CREATE TABLE vertex_md5_cache (id INT, md5 CHAR(32))" +
                    " WITH BLOCKPROPERTIES (TYPE columnstore);");

        int counter = lastNumVertices;
        vertexMD5.setLength(0);
        vertexMD5.append("\0COPY vertex_md5_cache FROM stdin WITH (DELIMITER '|');\n\0");
        for (AbstractVertex v : vertices) {
          ++counter;
          vertexMD5.append(String.valueOf(counter));
          vertexMD5.append("|");
          vertexMD5.append(v.bigHashCode());
          vertexMD5.append('\n');
        }
        submitQuery(vertexMD5.toString());

        submitQuery("INSERT INTO vertex SELECT * FROM vertex_md5_cache;\n" +
                    "INSERT INTO trace_base_vertex SELECT id FROM vertex_md5_cache;");

        counter = lastNumVertices;
        vertexAnnos.setLength(0);
        vertexAnnos.append("\0COPY vertex_anno FROM stdin WITH (DELIMITER '|');\n\0");
        for (AbstractVertex v : vertices) {
          ++counter;
          String id = String.valueOf(counter);
          for (Map.Entry<String, String> annoEntry : v.getAnnotations().entrySet()) {
            vertexAnnos.append(id);
            vertexAnnos.append('|');
            vertexAnnos.append(annoEntry.getKey());
            vertexAnnos.append('|');
            appendEscaped(vertexAnnos, annoEntry.getValue());
            vertexAnnos.append('\n');
          }
        }
        submitQuery(vertexAnnos.toString());
      }

      if (edges.size() > 0) {
        int lastNumEdges;
        try {
          String sn = executeQuery("COPY SELECT COUNT(*) FROM edge TO stdout;");
          lastNumEdges = Integer.parseInt(sn);
        } catch (Exception e) {
          logger.log(Level.SEVERE, e.getMessage());
          return;
        }
        final int startId = lastNumEdges + 1;

        submitQuery("DROP TABLE edge_md5_cache;\n" +
                    "CREATE TABLE edge_md5_cache (idx INT, src CHAR(32), dst CHAR(32))" +
                    " WITH BLOCKPROPERTIES (TYPE columnstore);");
        edgeMD5.setLength(0);
        edgeMD5.append("\0COPY edge_md5_cache FROM stdin WITH (DELIMITER '|');\n\0");
        for (int i = 0; i < edges.size(); ++i) {
          edgeMD5.append(i);
          edgeMD5.append('|');
          edgeMD5.append(edges.get(i).getChildVertex().bigHashCode());
          edgeMD5.append('|');
          edgeMD5.append(edges.get(i).getParentVertex().bigHashCode());
          edgeMD5.append('\n');
        }
        submitQuery(edgeMD5.toString());

        submitQuery("INSERT INTO trace_base_edge" +
                    " SELECT idx + " + startId + " FROM edge_md5_cache;");

        submitQuery("\\analyzecount vertex edge_md5_cache\n" +
                    "DROP TABLE unique_md5;\n" +
                    "CREATE TABLE unique_md5 (md5 CHAR(32))" +
                    " WITH BLOCKPROPERTIES (TYPE columnstore);\n" +
                    "INSERT INTO unique_md5 SELECT md5 FROM" +
                    " (SELECT src AS md5 FROM edge_md5_cache UNION ALL" +
                    "  SELECT dst AS md5 FROM edge_md5_cache) t GROUP BY md5;\n" +
                    "\\analyzecount unique_md5\n" +
                    "DROP TABLE vertex_md5_cache;\n" +
                    "CREATE TABLE vertex_md5_cache (id INT, md5 CHAR(32))" +
                    " WITH BLOCKPROPERTIES (TYPE columnstore);\n" +
                    "INSERT INTO vertex_md5_cache SELECT id, md5 FROM vertex" +
                    " WHERE md5 IN (SELECT md5 FROM unique_md5);\n" +
                    "\\analyzecount vertex_md5_cache\n");

        String rs = executeQuery(
            "COPY SELECT c.idx, s.id, d.id" +
            "     FROM   vertex_md5_cache s, vertex_md5_cache d, edge_md5_cache c" +
            "     WHERE  s.md5 = c.src AND d.md5 = c.dst" +
            " TO stdout WITH (DELIMITER e'\\n');");
        String edgeIdx[] = rs.split("\n");
        rs = null;

        assert edgeIdx.length % 3 == 0;
        for (int i = 0; i < edgeIdx.length; i += 3) {
          int rowIdx = Integer.parseInt(edgeIdx[i]);
          edgeIdPairs[rowIdx][0] = Integer.parseInt(edgeIdx[i+1]);
          edgeIdPairs[rowIdx][1] = Integer.parseInt(edgeIdx[i+2]);
        }
        edgeIdx = null;

        edgeLinks.setLength(0);
        edgeLinks.append("\0COPY edge FROM stdin WITH (DELIMITER '|');\n\0");
        for (int i = 0; i < edges.size(); ++i) {
          edgeLinks.append(String.valueOf(startId + i));
          edgeLinks.append('|');
          edgeLinks.append(edgeIdPairs[i][0]);
          edgeLinks.append('|');
          edgeLinks.append(edgeIdPairs[i][1]);
          edgeLinks.append('\n');
        }
        submitQuery(edgeLinks.toString());

        edgeAnnos.setLength(0);
        edgeAnnos.append("\0COPY edge_anno FROM stdin WITH (DELIMITER '|');\n\0");
        for (int i = 0; i < edges.size(); ++i) {
          String id = String.valueOf(startId + i);
          for (Map.Entry<String, String> annoEntry : edges.get(i).getAnnotations().entrySet()) {
            edgeAnnos.append(id);
            edgeAnnos.append('|');
            appendEscaped(edgeAnnos, annoEntry.getKey());
            edgeAnnos.append('|');
            appendEscaped(edgeAnnos, annoEntry.getValue());
            edgeAnnos.append('\n');
          }
        }
        submitQuery(edgeAnnos.toString());
      }

      // For stable measurement of loading time, we just finalize each batch here ...
      finalizeQuery();

      logInfo("Done processing batch " + batchBuffer.getBatchID() + " at " +
              formatTime(System.currentTimeMillis() - timeExecutionStart));
    }

    private void submitQuery(String query) {
      finalizeQuery();
      queryFuture = queryExecutor.submit(new QsQuery(query));
    }

    private String finalizeQuery() {
      String result = null;
      if (queryFuture != null) {
        try {
          result = queryFuture.get();
        } catch (InterruptedException | ExecutionException e) {
          logger.log(Level.SEVERE, e.getMessage());
        }
        queryFuture = null;
      }
      return result;
    }

    public String executeQuery(String query) {
      submitQuery(query);
      return finalizeQuery();
    }

    private void appendEscaped(StringBuilder sb, String str) {
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

    private String formatTime(long milliseconds) {
      long time = milliseconds / 1000;
      StringBuilder sb = new StringBuilder();
      int[] divs = new int[] {
          86400, 3600, 60, 1
      };
      String[] units = new String[] {
          "d", "h", "m", "s"
      };
      for (int i = 0; i < divs.length; ++i) {
        if (time >= divs[i]) {
          sb.append(time / divs[i]);
          sb.append(units[i]);
          time %= divs[i];
        }
      }
      if (sb.length() == 0) {
        sb.append("" + (milliseconds % 1000) + "ms");
      }
      return sb.toString();
    }
  }

  private GraphBatch batch = new GraphBatch();
  private QsExecutor qs = new QsExecutor();

  private Timer forceSubmitTimer;
  private int forceSubmitTimeInterval = DEFAULT_FORCE_SUBMIT_TIME_INTERVAL;

  @Override
  public boolean initialize(String arguments) {
    HashMap<String, String> argMap = ParseKeyValuePairs(arguments);
    clientPath = argMap.get("clientPath");
    if (clientPath == null) {
      logger.log(Level.SEVERE, "Parameter required: clientPath");
      return false;
    }

    // Maybe null.
    serverIP = argMap.get("serverIP");
    serverPort = argMap.get("serverPort");
    infoPath = argMap.get("infoPath");

    // Set edge batch size.
    String strEdgeBatchSize = argMap.get("batchSize");
    if (strEdgeBatchSize != null) {
      try {
        edgeBatchSize = Integer.parseInt(strEdgeBatchSize);
      } catch (Exception e) {
        logger.log(Level.SEVERE, "Invalid parameter value: batchSize");
        return false;
      }
      if (edgeBatchSize < MINIMUM_EDGE_BATCH_SIZE) {
        edgeBatchSize = MINIMUM_EDGE_BATCH_SIZE;
        logger.log(Level.WARNING, "batchSize too low, adjusted to " + edgeBatchSize);
      }
    }

    // Set force submit time interval.
    String strTimeInterval = argMap.get("batchTimeIntervalInSeconds");
    if (strTimeInterval != null) {
      try {
        forceSubmitTimeInterval = Integer.parseInt(strTimeInterval);
      } catch (Exception e) {
        logger.log(Level.SEVERE, "Invalid parameter value: batchTimeIntervalInSeconds");
        return false;
      }
      if (forceSubmitTimeInterval < MINIMUM_FORCE_SUBMIT_TIME_INTERVAL) {
        forceSubmitTimeInterval = MINIMUM_FORCE_SUBMIT_TIME_INTERVAL;
        logger.log(Level.WARNING, "batchTimeIntervalInSeconds too low, adjusted to " + forceSubmitTimeInterval);
      }
    }

    // Initialize info writer.
    if (infoPath != null) {
      try {
        infoWriter = new PrintWriter(new File(infoPath));
      } catch (FileNotFoundException e) {
        infoWriter = null;
      }
    }

    // Initialize Quickstep async executor.
    qs.initialize();

    // Reset Quickstep storage if required.
    String reset = argMap.get("reset");
    if (reset != null && reset.equalsIgnoreCase("true")) {
      qs.resetStorage();
    } else {
      qs.resetStorageIfInvalid();
    }

    timeExecutionStart = System.currentTimeMillis();
    resetForceSubmitTimer();

    return true;
  }

  @Override
  public boolean shutdown() {
    if (!batch.isEmpty()) {
      batch = qs.submitBatch(batch);
      qs.finalizeBatch();
    }
    qs.shutdown();
    if (infoWriter != null) {
      infoWriter.close();
      infoWriter = null;
    }
    return true;
  }

  @Override
  public AbstractEdge getEdge(String childVertexHash, String parentVertexHash) {
    logger.log(Level.SEVERE, "Not supported");
    return null;
  }

  @Override
  public AbstractVertex getVertex(String vertexHash) {
    logger.log(Level.SEVERE, "Not supported");
    return null;
  }

  @Override
  public Graph getChildren(String parentHash) {
    logger.log(Level.SEVERE, "Not supported");
    return null;
  }

  @Override
  public Graph getParents(String childVertexHash) {
    logger.log(Level.SEVERE, "Not supported");
    return null;
  }

  @Override
  public boolean putEdge(AbstractEdge incomingEdge) {
    synchronized (batch) {
      batch.addEdge(incomingEdge);
      if (batch.getEdges().size() >= edgeBatchSize) {
        batch = qs.submitBatch(batch);
        resetForceSubmitTimer();
      }
    }
    return true;
  }

  @Override
  public boolean putVertex(AbstractVertex incomingVertex) {
    synchronized (batch) {
      batch.addVertex(incomingVertex);
    }
    return true;
  }

  @Override
  public Object executeQuery(String query) {
    return qs.executeQuery(query);
  }

  public String execute(String query) {
    return qs.executeQuery(query);
  }

  public long executeForLongResult(String query) {
    String resultStr = qs.executeQuery(query).trim();
    try {
      return Long.parseLong(resultStr);
    } catch (Exception e) {
      throw new RuntimeException(
          "Unexpected result \"" + resultStr + "\" from Quickstep: expecting an integer");
    }
  }

  public void beginTransaction() {
    qs.beginTransaction();
  }

  public void finalizeTransaction() {
    qs.finalizeTransction();
  }

  public ArrayList<String> getAllTableNames() {
    return qs.getAllTableNames();
  }

  private void resetForceSubmitTimer() {
    if (forceSubmitTimer != null) {
      forceSubmitTimer.cancel();
    }
    forceSubmitTimer = new Timer();
    forceSubmitTimer.schedule(new TimerTask() {
      @Override
      public void run() {
        synchronized (batch) {
          if (!batch.isEmpty()) {
            batch = qs.submitBatch(batch);
          }
          resetForceSubmitTimer();
        }
      }
    }, forceSubmitTimeInterval * 1000);
  }

  private static HashMap<String, String> ParseKeyValuePairs(String arguments) {
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
}

