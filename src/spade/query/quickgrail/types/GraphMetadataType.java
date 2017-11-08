package spade.query.quickgrail.types;

import spade.query.quickgrail.entities.GraphMetadata;

public class GraphMetadataType extends Type {
  static private GraphMetadataType instance;

  public static GraphMetadataType GetInstance() {
    if (instance == null) {
      instance = new GraphMetadataType();
    }
    return instance;
  }

  @Override
  public TypeID getTypeID() {
    return TypeID.kGraphMetadata;
  }

  @Override
  public String getName() {
    return "GraphMetadata";
  }

  @Override
  public String printValueToString(Object value) {
    assert (value instanceof GraphMetadata);
    return ((GraphMetadata)value).getName();
  }
}
