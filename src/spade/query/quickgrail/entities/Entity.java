package spade.query.quickgrail.entities;

import spade.query.quickgrail.utility.TreeStringSerializable;

public abstract class Entity extends TreeStringSerializable {
  public abstract EntityType getEntityType();
}
