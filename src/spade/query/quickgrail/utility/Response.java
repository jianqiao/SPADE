package spade.query.quickgrail.utility;

import java.io.Serializable;
import java.util.HashMap;

public class Response implements Serializable {
  private static final long serialVersionUID = 1L;
  private Object result;
  private HashMap<String, Object> metaData = new HashMap<String, Object>();

  public Response(Object result) {
    this.result = result;
  }

  public Object getResult() {
    return result;
  }

  public void setMetaData(String name, Object value) {
    metaData.put(name, value);
  }

  public HashMap<String, Object> getMetaData() {
    return metaData;
  }
}
