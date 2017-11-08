package spade.query.quickgrail.execution;

import java.util.ArrayList;

import spade.storage.Quickstep;

public class ExecutionContext {
  private Quickstep qs;
  private ArrayList<Object> responses;

  public ExecutionContext(Quickstep qs) {
    this.qs = qs;
    this.responses = new ArrayList<Object>();
  }

  public Quickstep getQuickstepInstance() {
    return qs;
  }

  public void addResponse(Object response) {
    responses.add(response);
  }

  public ArrayList<Object> getResponses() {
    return responses;
  }
}
