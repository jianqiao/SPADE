package spade.query.quickgrail.entities;

import java.util.ArrayList;

import spade.query.quickgrail.utility.TreeStringSerializable;

public class GraphMetadata extends Entity {
  private String name;

  public GraphMetadata(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public String getVertexTableName() {
    return name + "_vertex";
  }

  public String getEdgeTableName() {
    return name + "_edge";
  }

  @Override
  public EntityType getEntityType() {
    return EntityType.kGraphMetadata;
  }

  @Override
  public String getLabel() {
    return "GraphMetadata";
  }

  @Override
  protected void getFieldStringItems(
      ArrayList<String> inline_field_names,
      ArrayList<String> inline_field_values,
      ArrayList<String> non_container_child_field_names,
      ArrayList<TreeStringSerializable> non_container_child_fields,
      ArrayList<String> container_child_field_names,
      ArrayList<ArrayList<? extends TreeStringSerializable>> container_child_fields) {
    inline_field_names.add("name");
    inline_field_values.add(name);
  }
}
