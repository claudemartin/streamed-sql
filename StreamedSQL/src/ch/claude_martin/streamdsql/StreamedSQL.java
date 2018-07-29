package ch.claude_martin.streamdsql;

import java.sql.*;
import java.util.stream.*;

/**
 * Utility class to easily create a Stream from an SQL statement.
 * 
 * <p>
 * {@link java.util.Spliterator#tryAdvance Spliterator.tryAdvance} can't throw
 * SQLException, so also catch {@link StreamedSQLException} to make sure you get
 * them all.
 * 
 * @author Claude Martin
 *
 */
public final class StreamedSQL {
  /**
   * Creates a prepared statement, which allows to get the row count. This might
   * result in better performance.
   * 
   * <p>
   * The given connection has to be open and ready. The returned statement will
   * not be opened yet.
   * 
   * <p>
   * Not that can just use {@link #stream(Connection, String, ResultSetMapper)}
   * directly, which will create a statement for you.
   * 
   * @param conn
   *          the connection
   * @param query
   *          the query statement
   * @return a prepared statement
   * @throws SQLException
   */
  public static PreparedStatement prepareStatement(Connection conn, String query) throws SQLException {
    PreparedStatement stmt = conn.prepareStatement(query, ResultSet.TYPE_SCROLL_INSENSITIVE,
        ResultSet.CONCUR_READ_ONLY);
    return stmt;
  }

  /**
   * Stream from a query string. The connection has to be open already and it will
   * not be closed.
   * 
   * <p>
   * Note that the stream has to be closed. Use <code>try-with-resource</code> to
   * make sure the stream does not remain open. The given connection will not be
   * closed.
   * 
   * @param conn
   *          the connection
   * @param query
   *          the query statement
   * @param mapper
   *          maps the {@link ResultSet} to some object
   * @return a parallel stream
   * @throws SQLException
   *           Thrown when the <i>query</i> can't be executed. Consider handling
   *           {@link StreamedSQLException} as well.
   */
  public static <T> Stream<T> stream(Connection conn, String query, ResultSetMapper<T> mapper) throws SQLException {
    return stream(prepareStatement(conn, query), mapper);
  }

  /**
   * Stream from a given prepared statement. The statement has to be ready to be
   * executed. For better performance it should be created as
   * <code>conn.prepareStatement(query, ResultSet.<b>TYPE_SCROLL_INSENSITIVE</b>, ResultSet.<b>CONCUR_READ_ONLY</b>);</code>.
   * 
   * <p>
   * Note that the stream has to be closed. Use <code>try-with-resource</code> to
   * make sure the stream does not remain open.
   * 
   * @param stmt
   *          the prepared statement
   * @param mapper
   *          maps the {@link ResultSet} to some object
   * @return a parallel stream
   * @throws SQLException
   *           Thrown when the <i>query</i> can't be executed. Consider handling
   *           {@link StreamedSQLException} as well.
   */
  public static <T> Stream<T> stream(PreparedStatement stmt, ResultSetMapper<T> mapper) throws SQLException {
    final ResultSet rs = stmt.executeQuery();
    try {
      long size;
      synchronized (rs) {
        try {
          rs.last();
          size = rs.getRow();
          rs.beforeFirst();
        } catch (Exception ex) {
          size = Long.MAX_VALUE;
        }
      }
      Stream<T> stream = StreamSupport.stream(new ResultSetSpliterator<T>(rs, size, mapper), true);
      stream.onClose(() -> {
        try {
          synchronized (rs) {
            stmt.close();
          }
        } catch (SQLException e) {
          throw new StreamedSQLException(e);
        }
      });
      return stream;
    } catch (Exception e) {
      try {
        stmt.close();
      } catch (SQLException e2) {
        e.addSuppressed(e);
      }
      throw e;
    }
  }
}
