package spade.filter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import org.apache.commons.codec.digest.DigestUtils;

import spade.core.AbstractEdge;
import spade.core.AbstractFilter;
import spade.core.AbstractVertex;

public class MergeEdges extends AbstractFilter {
  private static class EdgeNode implements Comparable<EdgeNode> {
    private AbstractEdge edge;
    private String sha256;
    private long recentUpdate;
    private long count;
    private boolean isActive = false;
    private EdgeNode prev = null;
    private EdgeNode next = null;

    public EdgeNode() {
      edge = null;
      sha256 = null;
      recentUpdate = 0;
      count = 0;
    }

    public EdgeNode(AbstractEdge edge, String sha256) {
      resetEdge(edge, sha256);
    }

    void clear() {
      edge = null;
      sha256 = null;
      prev = null;
      next = null;
    }

    public void resetEdge(AbstractEdge edge, String sha256) {
      this.edge = edge;
      this.sha256 = sha256;
      recentUpdate = System.currentTimeMillis();
      count = 1;
    }

    public void increaseCount() {
      recentUpdate = System.currentTimeMillis();
      ++count;
    }

    @Override
    public int compareTo(EdgeNode node) {
      if (recentUpdate > node.recentUpdate) {
        return -1;
      } else if (recentUpdate < node.recentUpdate) {
        return 1;
      } else {
        return 0;
      }
    }
  }

  private static final int POOL_SIZE = 8 * 1024 * 1024;

  private ArrayList<EdgeNode> activePool = new ArrayList<EdgeNode>();
  private EdgeNode evictionQueue = new EdgeNode();
  private HashMap<String, EdgeNode> edgeMap = new HashMap<String, EdgeNode>();

  private int edgeCount;

  @Override
  public boolean initialize(String arguments) {
    edgeCount = 0;
    return true;
  }

  @Override
  public boolean shutdown() {
    for (EdgeNode node : activePool) {
      commit(node);
      node.clear();
    }
    activePool.clear();
    for (EdgeNode node = evictionQueue.next; node != null; node = node.next) {
      commit(node);
      node.clear();
    }
    evictionQueue.clear();
    return true;
  }

  @Override
  public void putVertex(AbstractVertex incomingVertex) {
    putInNextFilter(incomingVertex);
  }

  @Override
  public void putEdge(AbstractEdge incomingEdge) {
    String edgeStr = incomingEdge.getSourceVertex().toString() + "|"
        + incomingEdge.getDestinationVertex().toString() + "|"
        + incomingEdge.toString();
    String edgeSha256 = DigestUtils.sha256Hex(edgeStr);
    EdgeNode node = edgeMap.get(edgeSha256);
    if (node == null) {
      if (edgeCount < POOL_SIZE) {
        node = new EdgeNode(incomingEdge, edgeSha256);
        ++edgeCount;
      } else {
        node = evictLRU();
        node.resetEdge(incomingEdge, edgeSha256);
      }
      node.isActive = true;
      activePool.add(node);
      edgeMap.put(edgeSha256, node);
    } else {
      if (!node.isActive) {
        removeNodeFromEvictionQueue(node);
        node.isActive = true;
        activePool.add(node);
      }
      node.increaseCount();
    }
  }

  private EdgeNode evictLRU() {
    if (evictionQueue.next == null) {
      Collections.sort(activePool);
      EdgeNode iter = evictionQueue;
      for (EdgeNode node : activePool) {
        node.isActive = false;
        iter.next = node;
        node.prev = iter;
        iter = node;
      }
      iter.next = null;
      activePool.clear();
    }

    EdgeNode node = evictionQueue.next;
    removeNodeFromEvictionQueue(node);
    commit(node);

    return node;
  }

  private void removeNodeFromEvictionQueue(EdgeNode node) {
    EdgeNode prevNode = node.prev;
    EdgeNode nextNode = node.next;
    prevNode.next = nextNode;
    if (nextNode != null) {
      nextNode.prev = prevNode;
    }
  }

  private void commit(EdgeNode node) {
    if (node.edge != null) {
      AbstractEdge edge = node.edge;
      if (node.count > 1) {
        edge.addAnnotation("rep", String.valueOf(node.count));
      }
      putInNextFilter(edge);
      node.edge = null;
      edgeMap.remove(node.sha256);
    }
  }
}