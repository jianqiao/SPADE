package spade.filter;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.codec.digest.DigestUtils;

import spade.core.AbstractEdge;
import spade.core.AbstractFilter;
import spade.core.AbstractVertex;

public class RetainKeys extends AbstractFilter {
  private Logger logger = Logger.getLogger(this.getClass().getName());
  private ArrayList<String> keysToRetain = new ArrayList<String>();

  @Override
  public boolean initialize(String arguments) {
    keysToRetain = new ArrayList<String>();
    keysToRetain.add("type");
    keysToRetain.add("commandline");
    return true;
  }

  @Override
  public void putVertex(AbstractVertex incomingVertex) {
    try {
      putInNextFilter(createCopyWithKeys(incomingVertex));
    } catch (InstantiationException | IllegalAccessException e) {
      logger.log(Level.SEVERE,
          "Failed to apply the RetainKeys filter to vertex: " + incomingVertex,
          e);
    }
  }

  @Override
  public void putEdge(AbstractEdge incomingEdge) {
    try {
      putInNextFilter(createCopyWithKeys(incomingEdge));
    } catch (InstantiationException | IllegalAccessException
        | IllegalArgumentException | InvocationTargetException
        | NoSuchMethodException | SecurityException e) {
      logger.log(Level.SEVERE,
          "Failed to apply the RetainKeys filter to edge: " + incomingEdge, e);
    }
  }

  private AbstractVertex createCopyWithKeys(AbstractVertex vertex)
      throws InstantiationException, IllegalAccessException {
    AbstractVertex vertexCopy = vertex.getClass().newInstance();
    for (String key : keysToRetain) {
      String value = vertex.getAnnotation(key);
      if (value != null) {
        vertexCopy.addAnnotation(key, value);
      }
    }
    vertexCopy.addAnnotation("_$sha256",
        DigestUtils.sha256Hex(vertex.toString()));
    return vertexCopy;
  }

  private AbstractEdge createCopyWithKeys(AbstractEdge edge)
      throws InstantiationException, IllegalAccessException,
      IllegalArgumentException, InvocationTargetException,
      NoSuchMethodException, SecurityException {
    AbstractVertex source = createCopyWithKeys(edge.getSourceVertex());
    AbstractVertex destination = createCopyWithKeys(edge.getDestinationVertex());
    AbstractEdge edgeCopy = edge.getClass()
        .getConstructor(source.getClass(), destination.getClass())
        .newInstance(source, destination);
    for (String key : keysToRetain) {
      String value = edge.getAnnotation(key);
      if (value != null) {
        edgeCopy.addAnnotation(key, value);
      }
    }
    return edgeCopy;
  }
}
