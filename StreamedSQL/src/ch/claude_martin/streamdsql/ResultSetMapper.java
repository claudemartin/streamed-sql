package ch.claude_martin.streamdsql;

import java.sql.*;
import java.util.function.*;

/**
 * Maps a ResultSet to some Object. it behaves just like a {@link Function}, but
 * it can throw an {@link SQLException}.
 */
public interface ResultSetMapper<T> {
  /** Map a result set to some Object. */
  abstract T map(ResultSet t) throws SQLException;

  /** Create a ResultSetMapper from some Function. */
  public static <T> ResultSetMapper<T> of(Function<ResultSet, T> f) {
    return rs -> f.apply(rs);
  }
}