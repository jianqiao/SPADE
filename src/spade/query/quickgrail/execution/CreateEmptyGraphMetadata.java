package spade.query.quickgrail.execution;

import java.util.ArrayList;

import spade.query.quickgrail.entities.GraphMetadata;
import spade.query.quickgrail.utility.QuickstepUtil;
import spade.query.quickgrail.utility.TreeStringSerializable;

public class CreateEmptyGraphMetadata extends Instruction {
  private GraphMetadata metadata;

  public CreateEmptyGraphMetadata(GraphMetadata metadata) {
    this.metadata = metadata;
  }

  @Override
  public void execute(ExecutionContext ctx) {
    QuickstepUtil.CreateEmptyGraphMetadata(ctx.getQuickstepInstance(), metadata);
  }

  @Override
  public String getLabel() {
    return "CreateEmptyGraphMetadata";
  }

  @Override
  protected void getFieldStringItems(
      ArrayList<String> inline_field_names,
      ArrayList<String> inline_field_values,
      ArrayList<String> non_container_child_field_names,
      ArrayList<TreeStringSerializable> non_container_child_fields,
      ArrayList<String> container_child_field_names,
      ArrayList<ArrayList<? extends TreeStringSerializable>> container_child_fields) {
    inline_field_names.add("metadata");
    inline_field_values.add(metadata.getName());
  }
}
