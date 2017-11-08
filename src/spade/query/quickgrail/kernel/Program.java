package spade.query.quickgrail.kernel;

import java.util.ArrayList;

import spade.query.quickgrail.execution.ExecutionContext;
import spade.query.quickgrail.execution.Instruction;
import spade.query.quickgrail.utility.TreeStringSerializable;
import spade.storage.Quickstep;

public class Program extends TreeStringSerializable {
  private ArrayList<Instruction> instructions;

  public Program(ArrayList<Instruction> instructions) {
    this.instructions = instructions;
  }

  public ArrayList<Object> execute(Quickstep qs) {
    ExecutionContext ctx = new ExecutionContext(qs);
    for (Instruction inst : instructions) {
      inst.execute(ctx);
    }
    return ctx.getResponses();
  }

  @Override
  public String getLabel() {
    return "Program";
  }

  @Override
  protected void getFieldStringItems(
      ArrayList<String> inline_field_names,
      ArrayList<String> inline_field_values,
      ArrayList<String> non_container_child_field_names,
      ArrayList<TreeStringSerializable> non_container_child_fields,
      ArrayList<String> container_child_field_names,
      ArrayList<ArrayList<? extends TreeStringSerializable>> container_child_fields) {
    container_child_field_names.add("instructions");
    container_child_fields.add(instructions);
  }
}
