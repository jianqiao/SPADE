/*
 --------------------------------------------------------------------------------
 SPADE - Support for Provenance Auditing in Distributed Environments.
 Copyright (C) 2015 SRI International

 This program is free software: you can redistribute it and/or
 modify it under the terms of the GNU General Public License as
 published by the Free Software Foundation, either version 3 of the
 License, or (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program. If not, see <http://www.gnu.org/licenses/>.
 --------------------------------------------------------------------------------
 */
package spade.storage;

import spade.core.AbstractEdge;
import spade.core.AbstractStorage;
import spade.core.AbstractVertex;
import spade.core.Graph;

import java.io.FileWriter;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TblFile extends AbstractStorage {

  private FileWriter vertexFile;
  private FileWriter vertexAnnoFile;
  private FileWriter edgeFile;
  private FileWriter edgeAnnoFile;
  private String dirPath;

  private long vertexId = 0;
  private long edgeId = 0;
  
  @Override
  public boolean initialize(String arguments) {
    try {
      if (arguments == null) {
        return false;
      }
      dirPath = arguments;
      vertexFile = new FileWriter(dirPath + "/vertex.tbl", false);
      vertexAnnoFile = new FileWriter(dirPath + "/vertex_anno.tbl", false);
      edgeFile = new FileWriter(dirPath + "/edge.tbl", false);
      edgeAnnoFile = new FileWriter(dirPath + "/edge_anno.tbl", false);
      return true;
    } catch (Exception exception) {
      Logger.getLogger(TblFile.class.getName()).log(Level.SEVERE, null, exception);
      return false;
    }
  }


  @Override
  public boolean shutdown() {
    try {
      vertexFile.close();
      vertexAnnoFile.close();
      edgeFile.close();
      edgeAnnoFile.close();
      return true;
    } catch (Exception exception) {
      Logger.getLogger(TblFile.class.getName()).log(Level.SEVERE, null, exception);
      return false;
    }
  }

  @Override
  public boolean putVertex(AbstractVertex incomingVertex) {
    try {
      String vertexMD5 = incomingVertex.bigHashCode();
      vertexFile.write((++vertexId) + "|" + vertexMD5 + "\n");

      for (Map.Entry<String, String> currentEntry : incomingVertex.getAnnotations().entrySet()) {
        String key = currentEntry.getKey();
        String value = currentEntry.getValue();
        if (key == null || value == null) {
          continue;
        }
        vertexAnnoFile.write(vertexMD5 + "|" + Escape(key) + "|" + Escape(value) + "\n");
      }
      return true;
    } catch (Exception exception) {
      Logger.getLogger(TblFile.class.getName()).log(Level.SEVERE, null, exception);
      return false;
    }
  }

  @Override
  public Object executeQuery(String query) {
    return null;
  }

  @Override
  public boolean putEdge(AbstractEdge incomingEdge) {
    try {
      String edgeMD5 = incomingEdge.bigHashCode();
      String srcMD5 = incomingEdge.getChildVertex().bigHashCode();
      String dstMD5 = incomingEdge.getParentVertex().bigHashCode();

      edgeFile.write((++edgeId) + "|" + edgeMD5 + "|" + srcMD5 + "|" + dstMD5 + "\n");
      
      for (Map.Entry<String, String> currentEntry : incomingEdge.getAnnotations().entrySet()) {
        String key = currentEntry.getKey();
        String value = currentEntry.getValue();
        if (key == null || value == null) {
          continue;
        }
        edgeAnnoFile.write(edgeMD5 + "|" + Escape(key) + "|" + Escape(value) + "\n");
      }
      return true;
    } catch (Exception exception) {
      Logger.getLogger(TblFile.class.getName()).log(Level.SEVERE, null, exception);
      return false;
    }
  }

  @Override
  public AbstractEdge getEdge(String childVertexHash, String parentVertexHash) {
    return null;
  }

  @Override
  public AbstractVertex getVertex(String vertexHash) {
    return null;
  }

  @Override
  public Graph getChildren(String parentHash) {
    return null;
  }

  @Override
  public Graph getParents(String childVertexHash) {
    return null;
  }

  private static String Escape(String str) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < str.length(); ++i) {
      char c = str.charAt(i);
      switch (c) {
        case '\\':
          sb.append("\\\\");
          break;
        case '|':
          sb.append("\\|");
          break;
        default:
          sb.append(c);
      }
    }
    return sb.toString();
  }
}
