package spade.query.quickgrail.parser;

import java.io.IOException;

import org.antlr.v4.runtime.BailErrorStrategy;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

public class DSLParserWrapper {
  public ParseProgram fromText(String text) {
    CharStream input = CharStreams.fromString(text);
    return fromCharStream(input);
  }

  public ParseProgram fromFile(String filename) throws IOException {
    CharStream input = CharStreams.fromFileName(filename);
    return fromCharStream(input);
  }

  public ParseProgram fromStdin() throws IOException {
    CharStream input = CharStreams.fromStream(System.in);
    return fromCharStream(input);
  }

  private ParseProgram fromCharStream(CharStream input) {
    DSLLexer lexer = new DSLLexer(input);
    DSLParser parser = new DSLParser(new CommonTokenStream(lexer));
    parser.setErrorHandler(new BailErrorStrategy());
    return parser.program().r;
  }
}
