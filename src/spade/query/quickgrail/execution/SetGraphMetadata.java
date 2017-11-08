package spade.query.quickgrail.execution;

import java.util.ArrayList;

import spade.query.quickgrail.entities.Graph;
import spade.query.quickgrail.entities.GraphMetadata;
import spade.query.quickgrail.utility.TreeStringSerializable;
import spade.storage.Quickstep;

public class SetGraphMetadata extends Instruction {
  private static final String kDigits = "0123456789ABCDEF";

  public enum Component {
    kVertex,
    kEdge,
    kBoth
  }

  private GraphMetadata targetMetadata;
  private Component component;
  private Graph sourceGraph;
  private String name;
  private String value;

  public SetGraphMetadata(GraphMetadata targetMetadata,
                          Component component,
                          Graph sourceGraph,
                          String name,
                          String value) {
    this.targetMetadata = targetMetadata;
    this.component = component;
    this.sourceGraph = sourceGraph;
    this.name = name;
    this.value = value;
  }

  @Override
  public void execute(ExecutionContext ctx) {
    Quickstep qs = ctx.getQuickstepInstance();

    String targetVertexTable = targetMetadata.getVertexTableName();
    String targetEdgeTable = targetMetadata.getEdgeTableName();
    String sourceVertexTable = sourceGraph.getVertexTableName();
    String sourceEdgeTable = sourceGraph.getEdgeTableName();

    qs.execute("\\analyzerange " + sourceVertexTable + " " + sourceEdgeTable + "\n");

    if (component == Component.kVertex || component == Component.kBoth) {
      qs.execute("INSERT INTO " + targetVertexTable +
                 " SELECT id, " + FormatStringLiteral(name) + ", " + FormatStringLiteral(value) +
                 " FROM " + sourceVertexTable + " GROUP BY id;");
    }

    if (component == Component.kEdge || component == Component.kBoth) {
      qs.execute("INSERT INTO " + targetEdgeTable +
                 " SELECT id, " + FormatStringLiteral(name) + ", " + FormatStringLiteral(value) +
                 " FROM " + sourceEdgeTable + " GROUP BY id;");
    }
  }

  private static String FormatStringLiteral(String input) {
    StringBuilder sb = new StringBuilder();
    sb.append("e'");
    for (int i = 0; i < input.length(); ++i) {
      char c = input.charAt(i);
      if (c >= 32) {
        if (c == '\\' || c == '\'') {
          sb.append(c);
        }
        sb.append(c);
        continue;
      }
      switch (c) {
        case '\b':
          sb.append("\\b");
          break;
        case '\n':
          sb.append("\\n");
          break;
        case '\r':
          sb.append("\\r");
          break;
        case '\t':
          sb.append("\\t");
          break;
        case '\\':
          sb.append("\\\\");
          break;
        default:
          // Use hexidecimal representation.
          sb.append("\\x");
          sb.append(kDigits.charAt(c >> 4));
          sb.append(kDigits.charAt(c & 0xF));
          break;
      }
    }
    sb.append("'");
    return sb.toString();
  }

  @Override
  public String getLabel() {
    return "SetGraphMetadata";
  }

  @Override
  protected void getFieldStringItems(
      ArrayList<String> inline_field_names,
      ArrayList<String> inline_field_values,
      ArrayList<String> non_container_child_field_names,
      ArrayList<TreeStringSerializable> non_container_child_fields,
      ArrayList<String> container_child_field_names,
      ArrayList<ArrayList<? extends TreeStringSerializable>> container_child_fields) {
    inline_field_names.add("targetMetadata");
    inline_field_values.add(targetMetadata.getName());
    inline_field_names.add("component");
    inline_field_values.add(component.name().substring(1));
    inline_field_names.add("sourceGraph");
    inline_field_values.add(sourceGraph.getName());
    inline_field_names.add("name");
    inline_field_values.add(name);
    inline_field_names.add("value");
    inline_field_values.add(value);
  }
}
