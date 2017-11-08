package spade.query.quickgrail.types;

public class IntegerType extends Type {
  static private IntegerType instance;

  public static IntegerType GetInstance() {
    if (instance == null) {
      instance = new IntegerType();
    }
    return instance;
  }

  @Override
  public TypeID getTypeID() {
    return TypeID.kInteger;
  }

  @Override
  public String getName() {
    return "Integer";
  }

  @Override
  public String printValueToString(Object value) {
    assert (value instanceof Integer);
    return String.valueOf(value);
  }
}
