package spade.query.quickgrail.execution;

import spade.query.quickgrail.utility.TreeStringSerializable;

public abstract class Instruction extends TreeStringSerializable {
  public abstract void execute(ExecutionContext ctx);
}
