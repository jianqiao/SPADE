package spade.client;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.HashMap;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;

import jline.ConsoleReader;
import spade.core.Graph;
import spade.core.Settings;

public class QuickGrailCommandLine {
  private static PrintStream clientOutputStream;
  private static ObjectInputStream clientInputStream;
  private static final String SPADE_ROOT = Settings.getProperty("spade_root");
  private static final String historyFile = SPADE_ROOT + "cfg/query.history";
  private static final String COMMAND_PROMPT = "-> ";

  // Members for creating secure sockets
  private static KeyStore clientKeyStorePrivate;
  private static KeyStore serverKeyStorePublic;
  private static SSLSocketFactory sslSocketFactory;

  private static void setupKeyStores() throws Exception {
    String serverPublicPath = SPADE_ROOT + "cfg/ssl/server.public";
    String clientPrivatePath = SPADE_ROOT + "cfg/ssl/client.private";

    serverKeyStorePublic = KeyStore.getInstance("JKS");
    serverKeyStorePublic.load(new FileInputStream(serverPublicPath), "public".toCharArray());
    clientKeyStorePrivate = KeyStore.getInstance("JKS");
    clientKeyStorePrivate.load(new FileInputStream(clientPrivatePath), "private".toCharArray());
  }

  private static void setupClientSSLContext() throws Exception {
    SecureRandom secureRandom = new SecureRandom();
    secureRandom.nextInt();

    TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
    tmf.init(serverKeyStorePublic);
    KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
    kmf.init(clientKeyStorePrivate, "private".toCharArray());

    SSLContext sslContext = SSLContext.getInstance("TLS");
    sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), secureRandom);
    sslSocketFactory = sslContext.getSocketFactory();
  }

  public static void main(String args[]) {
    // Set up context for secure connections
    try {
      setupKeyStores();
      setupClientSSLContext();
    } catch (Exception ex) {
      System.err.println(QuickGrailCommandLine.class.getName() +
                         " Error setting up context for secure connection. " + ex);
    }

    try {
      String host = "localhost";
      int port = Integer.parseInt(Settings.getProperty("commandline_query_port"));
      SSLSocket remoteSocket = (SSLSocket) sslSocketFactory.createSocket(host, port);

      OutputStream outStream = remoteSocket.getOutputStream();
      InputStream inStream = remoteSocket.getInputStream();
      clientInputStream = new ObjectInputStream(inStream);
      clientOutputStream = new PrintStream(outStream);
    } catch (NumberFormatException | IOException ex) {
      System.err.println(QuickGrailCommandLine.class.getName() +
                         " Error connecting to SPADE! " + ex);
      System.exit(-1);
    }

    try {
      System.out.println("SPADE 3.0 Query Client");

      // Set up command history and tab completion.
      ConsoleReader commandReader = new ConsoleReader();
      try {
        commandReader.getHistory().setHistoryFile(new File(historyFile));
      } catch (Exception ex) {
        System.err.println(QuickGrailCommandLine.class.getName() + " Command history not set up! " + ex);
      }

      while (true) {
        try {
          System.out.print(COMMAND_PROMPT);
          String line = commandReader.readLine().trim();
          if (line.isEmpty()) {
            continue;
          }

          // send line to analyzer
          long start_time = System.currentTimeMillis();
          clientOutputStream.println(line);
          String returnType = (String)clientInputStream.readObject();
          @SuppressWarnings("unchecked")
          HashMap<String, Object> metaData = (HashMap<String, Object>)clientInputStream.readObject();
          Object metaType = metaData.get("type");
          Object result = clientInputStream.readObject();
          long elapsed_time = System.currentTimeMillis() - start_time;
          System.out.println("Time taken for query: " + elapsed_time + " ms");

          System.out.println();
          if (metaType == null) {
            System.out.println("Return type: " + returnType);
            System.out.println("Result: " + result.toString());
          } else {
            System.out.println("Return type: " + metaType);
            switch (metaType.toString()) {
              case "Export": {
                String path = (String)metaData.get("path");
                FileWriter writer = new FileWriter(path, false);
                writer.write(((Graph)result).exportGraph());
                writer.flush();
                writer.close();
                System.out.println("Result: output exported to " + path);
                break;
              }
              case "Error": {
                System.out.println("Result: " + (String)result);
                break;
              }
              default: {
                System.out.println("Result: unknown data");
                break;
              }
            }
          }
          System.out.println("------------------");
        }
        catch (Exception ex) {
          System.err.println(QuickGrailCommandLine.class.getName() +
                             " Error talking to the client! " + ex);
          ex.printStackTrace();
        }
      }
    } catch (IOException ex) {
      System.err.println(QuickGrailCommandLine.class.getName() +
                         " Error in CommandLine Client! " + ex);
    }
  }
}
