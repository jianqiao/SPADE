package spade.query.quickgrail.execution;

import java.util.ArrayList;

import spade.query.quickgrail.entities.Graph;
import spade.query.quickgrail.utility.QuickstepUtil;
import spade.query.quickgrail.utility.TreeStringSerializable;

public class CreateEmptyGraph extends Instruction {
  private Graph graph;

  public CreateEmptyGraph(Graph graph) {
    this.graph = graph;
  }

  @Override
  public void execute(ExecutionContext ctx) {
    QuickstepUtil.CreateEmptyGraph(ctx.getQuickstepInstance(), graph);
  }

  @Override
  public String getLabel() {
    return "CreateEmptyGraph";
  }

  @Override
  protected void getFieldStringItems(
      ArrayList<String> inline_field_names,
      ArrayList<String> inline_field_values,
      ArrayList<String> non_container_child_field_names,
      ArrayList<TreeStringSerializable> non_container_child_fields,
      ArrayList<String> container_child_field_names,
      ArrayList<ArrayList<? extends TreeStringSerializable>> container_child_fields) {
    inline_field_names.add("graph");
    inline_field_values.add(graph.getName());
  }
}
