package spade.query.quickgrail.types;

public class StringType extends Type {
  static private StringType instance;

  public static StringType GetInstance() {
    if (instance == null) {
      instance = new StringType();
    }
    return instance;
  }

  @Override
  public TypeID getTypeID() {
    return TypeID.kString;
  }

  @Override
  public String getName() {
    return "String";
  }

  @Override
  public String printValueToString(Object value) {
    assert (value instanceof String);
    return (String)value;
  }
}
