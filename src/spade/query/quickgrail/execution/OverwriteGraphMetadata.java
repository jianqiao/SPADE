package spade.query.quickgrail.execution;

import java.util.ArrayList;

import spade.query.quickgrail.entities.GraphMetadata;
import spade.query.quickgrail.utility.TreeStringSerializable;
import spade.storage.Quickstep;

public class OverwriteGraphMetadata extends Instruction {
  private GraphMetadata targetMetadata;
  private GraphMetadata lhsMetadata;
  private GraphMetadata rhsMetadata;

  public OverwriteGraphMetadata(GraphMetadata targetMetadata,
                                GraphMetadata lhsMetadata,
                                GraphMetadata rhsMetadata) {
    this.targetMetadata = targetMetadata;
    this.lhsMetadata = lhsMetadata;
    this.rhsMetadata = rhsMetadata;
  }

  @Override
  public void execute(ExecutionContext ctx) {
    Quickstep qs = ctx.getQuickstepInstance();

    String targetVertexTable = targetMetadata.getVertexTableName();
    String targetEdgeTable = targetMetadata.getEdgeTableName();
    String lhsVertexTable = lhsMetadata.getVertexTableName();
    String lhsEdgeTable = lhsMetadata.getEdgeTableName();
    String rhsVertexTable = rhsMetadata.getVertexTableName();
    String rhsEdgeTable = rhsMetadata.getEdgeTableName();

    qs.execute("\\analyzerange " + rhsVertexTable + " " + rhsEdgeTable + "\n" +
               "INSERT INTO " + targetVertexTable +
               " SELECT id, name, value FROM " + lhsVertexTable + " l" +
               " WHERE NOT EXISTS (SELECT * FROM " + rhsVertexTable + " r" +
               " WHERE l.id = r.id AND l.name = r.name);\n" +
               "INSERT INTO " + targetEdgeTable +
               " SELECT id, name, value FROM " + lhsEdgeTable + " l" +
               " WHERE NOT EXISTS (SELECT * FROM " + rhsEdgeTable + " r" +
               " WHERE l.id = r.id AND l.name = r.name);\n" +
               "INSERT INTO " + targetVertexTable +
               " SELECT id, name, value FROM " + rhsVertexTable + ";\n" +
               "INSERT INTO " + targetEdgeTable +
               " SELECT id, name, value FROM " + rhsEdgeTable + ";");
  }

  @Override
  public String getLabel() {
    return "OverwritehGraphMetadata";
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
    inline_field_names.add("lhsMetadata");
    inline_field_values.add(lhsMetadata.getName());
    inline_field_names.add("rhsMetadata");
    inline_field_values.add(rhsMetadata.getName());
  }
}
