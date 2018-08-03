package ch.claude_martin.streamedsql;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;

/**
 * The {@link Spliterator} used to stream a {@link ResultSet}.
 * 
 * The estimated size must be either accurate or, if the size is unknown,
 * <code>Long.MAX_VALUE</code> .
 */
final class ResultSetSpliterator<T> extends Spliterators.AbstractSpliterator<T> {
  private final ResultSet rs;
  private final ResultSetMapper<T> mapper;

  /**
   * Create a new ResultSetSpliterator.
   * 
   * @param rs
   *          The result set
   * @param size
   *          the estimated size
   * @param mapper
   *          used to generate objects from database rows
   */
  ResultSetSpliterator(final ResultSet rs, final long size, final ResultSetMapper<T> mapper) {
    super(size, ResultSetSpliterator.ORDERED | (size < Long.MAX_VALUE ? ResultSetSpliterator.SIZED : 0));
    if (size < 0)
      throw new IllegalArgumentException("size must be positive. Use Long.MAX_VALUE if unknown.");
    this.rs = Objects.requireNonNull(rs);
    this.mapper = Objects.requireNonNull(mapper);
  }

  @Override
  public boolean tryAdvance(final Consumer<? super T> action) {
    try {
      if (!rs.next())
        return false;
      action.accept(mapper.map(rs));
      return true;
    } catch (SQLException e) {
      throw new StreamedSQLException(e);
    }
  }
}