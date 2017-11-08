package spade.query.quickgrail;

import java.util.ArrayList;

import spade.core.AbstractQuery;
import spade.query.quickgrail.kernel.Environment;
import spade.query.quickgrail.kernel.Program;
import spade.query.quickgrail.kernel.Resolver;
import spade.query.quickgrail.parser.DSLParserWrapper;
import spade.query.quickgrail.parser.ParseProgram;
import spade.storage.Quickstep;

public class QuickGrail extends AbstractQuery<Object, String> {
  @Override
  public Object execute(String query, Integer limit) {
    if (currentStorage == null ||
        !currentStorage.getClass().getSimpleName().equals("Quickstep")) {
      throw new RuntimeException("Cannot not find Quickstep storage");
    }
    Quickstep qs = (Quickstep)currentStorage;

    DSLParserWrapper parserWrapper = new DSLParserWrapper();
    ParseProgram parseProgram = parserWrapper.fromText(query);

    qs.logInfo("Parse tree:\n" + parseProgram.toString());

    Environment env = new Environment(qs);

    Resolver resolver = new Resolver();
    Program program = resolver.resolveProgram(parseProgram, env);

    qs.logInfo("Execution plan:\n" + program.toString());

    ArrayList<Object> responses;
    qs.beginTransaction();
    try {
      responses = program.execute(qs);
    } finally {
      qs.finalizeTransaction();
      env.gc();
    }

    if (responses == null || responses.isEmpty()) {
      return "OK";
    } else {
      // Currently only return the last response.
      return responses.get(responses.size()-1);
    }
  }
}
