package spade.query.quickgrail.types;

public abstract class Type {
  public abstract TypeID getTypeID();
  public abstract String getName();
  public abstract String printValueToString(Object value);
}
