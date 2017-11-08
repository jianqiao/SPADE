package spade.query.quickgrail.entities;

import java.util.ArrayList;

import spade.query.quickgrail.utility.TreeStringSerializable;

public class Graph extends Entity {
  public enum Component {
    kVertex,
    kEdge
  }

  public static String GetBaseVertexTableName() {
    return "vertex";
  }

  public static String GetBaseVertexAnnotationTableName() {
    return "vertex_anno";
  }

  public static String GetBaseEdgeTableName() {
    return "edge";
  }

  public static String GetBaseEdgeAnnotationTableName() {
    return "edge_anno";
  }

  public static String GetBaseTableName(Component component) {
    return component == Component.kVertex ? GetBaseVertexTableName() : GetBaseEdgeTableName();
  }

  public static String GetBaseAnnotationTableName(Component component) {
    return component == Component.kVertex
        ? GetBaseVertexAnnotationTableName() :
          GetBaseEdgeAnnotationTableName();
  }


  private String name;

  public Graph(String name) {
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

  public String getTableName(Component component) {
    return component == Component.kVertex ? getVertexTableName() : getEdgeTableName();
  }

  @Override
  public EntityType getEntityType() {
    return EntityType.kGraph;
  }


  @Override
  public String getLabel() {
    return "Graph";
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
