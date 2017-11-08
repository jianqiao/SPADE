package spade.query.quickgrail.types;

import spade.query.quickgrail.entities.Graph;

public class GraphType extends Type {
  static private GraphType instance;

  public static GraphType GetInstance() {
    if (instance == null) {
      instance = new GraphType();
    }
    return instance;
  }

  @Override
  public TypeID getTypeID() {
    return TypeID.kGraph;
  }

  @Override
  public String getName() {
    return "Graph";
  }

  @Override
  public String printValueToString(Object value) {
    assert (value instanceof Graph);
    return ((Graph)value).getName();
  }
}
