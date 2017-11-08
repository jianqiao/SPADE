package spade.query.quickgrail.kernel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;

import spade.query.quickgrail.entities.Graph;
import spade.query.quickgrail.entities.GraphMetadata;
import spade.query.quickgrail.utility.QuickstepUtil;
import spade.query.quickgrail.utility.TreeStringSerializable;
import spade.storage.Quickstep;

public class Environment extends TreeStringSerializable {
  private final static Graph kBaseGraph = new Graph("trace_base");

  private HashMap<String, String> symbols;
  private Quickstep qs;

  public Environment(Quickstep qs) {
    this.qs = qs;
    this.symbols = new HashMap<String, String>();
    // Initialize the symbols table if it does not exist.
    String probeOutput = qs.execute("SELECT COUNT(*) FROM symbols;");
    if (probeOutput == null || probeOutput.contains("ERROR")) {
      qs.execute("CREATE TABLE symbols (name VARCHAR(128), value VARCHAR(128));");
    }

    // Initialize the symbols buffer.
    String lines = qs.execute("COPY SELECT * FROM symbols TO stdout WITH (DELIMITER ',');");
    for (String line : lines.split("\n")) {
      String[] items = line.split(",");
      if (items.length == 2) {
        symbols.put(items[0], items[1]);
      }
    }
  }

  public void clear() {
    qs.execute("DROP TABLE symbols;");
    qs.execute("CREATE TABLE symbols (name VARCHAR(128), value VARCHAR(128));");
    symbols.clear();
    gc();
  }

  public Graph allocateGraph() {
    String idCounterStr = symbols.get("id_counter");
    if (idCounterStr == null) {
      idCounterStr = "0";
    }
    int idCounter = Integer.parseInt(idCounterStr);
    String nextIdStr = String.valueOf(++idCounter);
    setValue("id_counter", nextIdStr);
    return new Graph("trace_" + nextIdStr);
  }

  public GraphMetadata allocateGraphMetadata() {
    String idCounterStr = symbols.get("id_counter");
    if (idCounterStr == null) {
      idCounterStr = "0";
    }
    int idCounter = Integer.parseInt(idCounterStr);
    String nextIdStr = String.valueOf(++idCounter);
    setValue("id_counter", nextIdStr);
    return new GraphMetadata("meta_" + nextIdStr);
  }

  public String lookup(String symbol) {
    switch (symbol) {
      case "$base": return kBaseGraph.getName();
    }
    return symbols.get(symbol);
  }

  public void setValue(String symbol, String value) {
    switch (symbol) {
      case "$base":
        throw new RuntimeException("Cannot reassign reserved variables.");
    }
    if (symbols.containsKey(symbol)) {
      qs.execute("UPDATE symbols SET value = '" + value +
                 "' WHERE name = '" + symbol + "';");
    } else {
      qs.execute("INSERT INTO symbols VALUES('" + symbol + "', '" + value + "');");
    }
    symbols.put(symbol, value);
  }

  private boolean isGarbageTable(HashSet<String> referencedTables, String table) {
    if (table.startsWith("m_")) {
      return true;
    }
    if (table.startsWith("trace") || table.startsWith("meta")) {
      return !referencedTables.contains(table);
    }
    return false;
  }

  public void gc() {
    HashSet<String> referencedTables = new HashSet<String>();
    referencedTables.add(kBaseGraph.getVertexTableName());
    referencedTables.add(kBaseGraph.getEdgeTableName());
    for (String graphName : symbols.values()) {
      Graph graph = new Graph(graphName);
      referencedTables.add(graph.getVertexTableName());
      referencedTables.add(graph.getEdgeTableName());
    }
    ArrayList<String> allTables = QuickstepUtil.GetAllTableNames(qs);
    StringBuilder dropQuery = new StringBuilder();
    for (String table : allTables) {
      if (isGarbageTable(referencedTables, table)) {
        dropQuery.append("DROP TABLE " + table + ";\n");
      }
    }
    if (dropQuery.length() > 0) {
      qs.execute(dropQuery.toString());
    }
  }

  public static boolean IsBaseGraph(Graph graph) {
    return graph.getName().equals(kBaseGraph.getName());
  }

  @Override
  public String getLabel() {
    return "Environment";
  }

  @Override
  protected void getFieldStringItems(
      ArrayList<String> inline_field_names,
      ArrayList<String> inline_field_values,
      ArrayList<String> non_container_child_field_names,
      ArrayList<TreeStringSerializable> non_container_child_fields,
      ArrayList<String> container_child_field_names,
      ArrayList<ArrayList<? extends TreeStringSerializable>> container_child_fields) {
    for (Entry<String, String> entry : symbols.entrySet()) {
      inline_field_names.add(entry.getKey());
      inline_field_values.add(entry.getValue());
    }
  }
}
