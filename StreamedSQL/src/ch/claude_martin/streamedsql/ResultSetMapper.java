package ch.claude_martin.streamedsql;

import java.sql.*;
import java.util.function.*;

/**
 * Maps a ResultSet to some Object. it behaves just like a {@link Function}, but
 * it can throw an {@link SQLException}.
 * 
 * The implementation should not have any side effects and it must not share the
 * reference to the {@link ResultSet} with other code, since it will be reused
 * to read the next row of data. The returned object must be thread-safe because
 * the stream is parallel by default.
 */
public interface ResultSetMapper<T> {
  /** Map a result set to some Object. */
  abstract T map(ResultSet t) throws SQLException;

  /** Create a ResultSetMapper from some Function. */
  public static <T> ResultSetMapper<T> of(Function<ResultSet, T> f) {
    return rs -> f.apply(rs);
  }
}