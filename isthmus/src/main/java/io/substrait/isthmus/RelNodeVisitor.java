package io.substrait.isthmus;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Intersect;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Match;
import org.apache.calcite.rel.core.Minus;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.core.Values;

/** A more generic version of RelShuttle that allows an alternative return value. */
public abstract class RelNodeVisitor<OUTPUT, EXCEPTION extends Throwable> {

  /**
   * @param scan TableScan object to visit
   * @return an object of type OUTPUT
   * @throws EXCEPTION throws an exception of type EXCEPTION if error occurred
   */
  public OUTPUT visit(TableScan scan) throws EXCEPTION {
    return visitOther(scan);
  }

  /**
   * @param scan TableFunctionScan object to visit
   * @return an object of type OUTPUT
   * @throws EXCEPTION throws an exception of type EXCEPTION if error occurred
   */
  public OUTPUT visit(TableFunctionScan scan) throws EXCEPTION {
    return visitOther(scan);
  }

  /**
   * @param values Values object to visit
   * @return an object of type OUTPUT
   * @throws EXCEPTION throws an exception of type EXCEPTION if error occurred
   */
  public OUTPUT visit(Values values) throws EXCEPTION {
    return visitOther(values);
  }

  /**
   * @param filter
   * @return an object of type OUTPUT
   * @throws EXCEPTION throws an exception of type EXCEPTION if error occurred
   */
  public OUTPUT visit(Filter filter) throws EXCEPTION {
    return visitOther(filter);
  }

  /**
   * @param calc Calc object to visit
   * @return an object of type OUTPUT
   * @throws EXCEPTION throws an exception of type EXCEPTION if error occurred
   */
  public OUTPUT visit(Calc calc) throws EXCEPTION {
    return visitOther(calc);
  }

  /**
   * @param project Project object to visit
   * @return an object of type OUTPUT
   * @throws EXCEPTION throws an exception of type EXCEPTION if error occurred
   */
  public OUTPUT visit(Project project) throws EXCEPTION {
    return visitOther(project);
  }

  /**
   * @param join Join object to visit
   * @return an object of type OUTPUT
   * @throws EXCEPTION throws an exception of type EXCEPTION if error occurred
   */
  public OUTPUT visit(Join join) throws EXCEPTION {
    return visitOther(join);
  }

  /**
   * @param correlate Correlate object to visit
   * @return an object of type OUTPUT
   * @throws EXCEPTION throws an exception of type EXCEPTION if error occurred
   */
  public OUTPUT visit(Correlate correlate) throws EXCEPTION {
    return visitOther(correlate);
  }

  /**
   * @param union Union object to visit
   * @return an object of type OUTPUT
   * @throws EXCEPTION throws an exception of type EXCEPTION if error occurred
   */
  public OUTPUT visit(Union union) throws EXCEPTION {
    return visitOther(union);
  }

  /**
   * @param intersect Intersect object to visit
   * @return an object of type OUTPUT
   * @throws EXCEPTION throws an exception of type EXCEPTION if error occurred
   */
  public OUTPUT visit(Intersect intersect) throws EXCEPTION {
    return visitOther(intersect);
  }

  /**
   * @param minus Minus object to visit
   * @return an object of type OUTPUT
   * @throws EXCEPTION throws an exception of type EXCEPTION if error occurred
   */
  public OUTPUT visit(Minus minus) throws EXCEPTION {
    return visitOther(minus);
  }

  /**
   * @param aggregate Aggregate object to visit
   * @return an object of type OUTPUT
   * @throws EXCEPTION throws an exception of type EXCEPTION if error occurred
   */
  public OUTPUT visit(Aggregate aggregate) throws EXCEPTION {
    return visitOther(aggregate);
  }

  /**
   * @param match Match object to visit
   * @return an object of type OUTPUT
   * @throws EXCEPTION throws an exception of type EXCEPTION if error occurred
   */
  public OUTPUT visit(Match match) throws EXCEPTION {
    return visitOther(match);
  }

  /**
   * @param sort Sort object to visit
   * @return an object of type OUTPUT
   * @throws EXCEPTION throws an exception of type EXCEPTION if error occurred
   */
  public OUTPUT visit(Sort sort) throws EXCEPTION {
    return visitOther(sort);
  }

  /**
   * @param exchange Exchange object to visit
   * @return an object of type OUTPUT
   * @throws EXCEPTION throws an exception of type EXCEPTION if error occurred
   */
  public OUTPUT visit(Exchange exchange) throws EXCEPTION {
    return visitOther(exchange);
  }

  /**
   * @param modify TableModify object to visit
   * @return an object of type OUTPUT
   * @throws EXCEPTION
   */
  public OUTPUT visit(TableModify modify) throws EXCEPTION {
    return visitOther(modify);
  }

  /**
   * @param other custom RelNode to visit
   * @return an object of type OUTPUT
   * @throws EXCEPTION
   */
  public abstract OUTPUT visitOther(RelNode other) throws EXCEPTION;

  /** A visitor for RelNode. */
  protected RelNodeVisitor() {}

  /**
   * The method you call when you would normally call RelNode.accept(visitor). Instead call
   * RelVisitor.reverseAccept(RelNode) due to the lack of ability to extend base classes.
   */
  public final OUTPUT reverseAccept(RelNode node) throws EXCEPTION {
    if (node instanceof TableScan scan) {
      return this.visit(scan);
    } else if (node instanceof TableFunctionScan scan) {
      return this.visit(scan);
    } else if (node instanceof Values values) {
      return this.visit(values);
    } else if (node instanceof Filter filter) {
      return this.visit(filter);
    } else if (node instanceof Calc calc) {
      return this.visit(calc);
    } else if (node instanceof Project project) {
      return this.visit(project);
    } else if (node instanceof Join join) {
      return this.visit(join);
    } else if (node instanceof Correlate correlate) {
      return this.visit(correlate);
    } else if (node instanceof Union union) {
      return this.visit(union);
    } else if (node instanceof Intersect intersect) {
      return this.visit(intersect);
    } else if (node instanceof Minus minus) {
      return this.visit(minus);
    } else if (node instanceof Match match) {
      return this.visit(match);
    } else if (node instanceof Sort sort) {
      return this.visit(sort);
    } else if (node instanceof Exchange exchange) {
      return this.visit(exchange);
    } else if (node instanceof Aggregate aggregate) {
      return this.visit(aggregate);
    } else if (node instanceof TableModify modify) {
      return this.visit(modify);
    } else {
      return this.visitOther(node);
    }
  }
}
