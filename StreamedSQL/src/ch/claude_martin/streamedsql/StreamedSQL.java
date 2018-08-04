package ch.claude_martin.streamedsql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * StreamedSQL allows you to easily create a parallel {@link Stream} from an SQL
 * statement (JDBC).
 * 
 * <p>
 * {@link java.util.Spliterator#tryAdvance Spliterator.tryAdvance} can't throw
 * SQLException, so also catch {@link StreamedSQLException} to make sure you get
 * them all.
 * 
 * <p>
 * Always use <code>try-with-resource</code>, so the resources get closed as
 * soon as possible. When passing a statement you have to close that. When
 * passing a query string you have to close the stream itself.
 * 
 * @author Claude Martin
 *
 */
public final class StreamedSQL {
  /** Default connection. */
  private final Optional<Connection> defConn;
  /** Parallel streams by default? */
  private final boolean defParallel;

  private StreamedSQL(final Connection conn, final boolean parallel) {
    this.defConn = Optional.ofNullable(conn);
    this.defParallel = parallel;
  }

  private Connection getDefConn() {
    return this.defConn.orElseThrow(() -> new IllegalStateException("No default connection available."));
  }

  /**
   * Returns a new {@link StreamedSQL}.
   * 
   * @return a new {@link StreamedSQL}
   */
  public static StreamedSQL create() {
    return new StreamedSQL(null, true);
  }

  /**
   * Returns a new {@link StreamedSQL} with a given default connection (which
   * could be <code>null</code>).
   * 
   * @param conn
   *          default connection (can be <code>null</code>)
   * @param parallel
   *          should the created streams be parallel by default?
   * @return a new {@link StreamedSQL}
   */
  public static StreamedSQL create(final Connection conn, final boolean parallel) {
    return new StreamedSQL(conn, parallel);
  }

  /**
   * Creates a prepared statement, which allows to get the row count. This might
   * result in better performance.
   * 
   * <p>
   * The given connection has to be open and ready. The returned statement will
   * not be opened yet.
   * 
   * <p>
   * Note that you can just use
   * {@link #stream(Connection, String, ResultSetMapper)} directly, which will
   * create a statement for you.
   * 
   * @param conn
   *          the connection
   * @param query
   *          the query statement
   * @return a prepared statement
   * @throws SQLException
   */
  public PreparedStatement prepareStatement(final Connection conn, final String query) throws SQLException {
    final var stmt = Objects.requireNonNull(conn, "conn").prepareStatement(Objects.requireNonNull(query, "query"),
        ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
    return stmt;
  }

  /**
   * Creates a prepared statement using the default connection, which allows to
   * get the row count. This might result in better performance.
   * 
   * @param query
   *          the query statement
   * @return a prepared statement
   * @throws SQLException
   *           if the query is not valid or the connection fails
   * @throws IllegalStateException
   *           if the default connection is not set
   */
  public PreparedStatement prepareStatement(final String query) throws SQLException, IllegalStateException {
    return prepareStatement(this.getDefConn(), query);
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
   * @return a parallel stream, which has to be closed
   * @throws SQLException
   *           Thrown when the <i>query</i> can't be executed. Consider handling
   *           {@link StreamedSQLException} as well.
   */
  public <T> Stream<T> stream(final Connection conn, final String query, final ResultSetMapper<T> mapper)
      throws SQLException {
    Objects.requireNonNull(conn, "conn");
    Objects.requireNonNull(query, "query");
    Objects.requireNonNull(mapper, "mapper");
    final PreparedStatement stmt = prepareStatement(conn, query);
    final Stream<T> stream = stream(stmt, mapper, this.defParallel);
    stream.onClose(() -> {
      try {
        stmt.close();
      } catch (SQLException e) {
        throw new StreamedSQLException(e);
      }
    }); // proper close by try-with-resource
    return stream;
  }

  /**
   * Stream from a query string using the default connection. The connection has
   * to be open already and it will not be closed.
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
   * @return a stream, which has to be closed
   * @throws SQLException
   *           Thrown when the <i>query</i> can't be executed. Consider handling
   *           {@link StreamedSQLException} as well.
   */
  public <T> Stream<T> stream(final String query, final ResultSetMapper<T> mapper) throws SQLException {
    return stream(this.getDefConn(), query, mapper);
  }

  /**
   * Stream from a given prepared statement. The statement has to be ready to be
   * executed. For better performance it should be created as
   * <code>conn.prepareStatement(query, ResultSet.<b>TYPE_SCROLL_INSENSITIVE</b>, ResultSet.<b>CONCUR_READ_ONLY</b>);</code>.
   * 
   * <p>
   * Note that the statement needs to be closed by the calling side. Use
   * <code>try-with-resource</code> to make sure the statement is always properly
   * closed. The stream returned here does not need to be closed.
   * 
   * @param stmt
   *          the prepared statement
   * @param mapper
   *          maps the {@link ResultSet} to some object
   * @param parallel
   *          if {@code true} then the returned stream is a parallel, else it is
   *          sequential.
   * @return a stream
   * @throws SQLException
   *           Thrown when the <i>query</i> can't be executed. Consider handling
   *           {@link StreamedSQLException} as well.
   */
  public <T> Stream<T> stream(final PreparedStatement stmt, final ResultSetMapper<T> mapper, final boolean parallel)
      throws SQLException, StreamedSQLException {
    Objects.requireNonNull(mapper, "mapper");
    // This result set is never closed, but the caller has to close the statement,
    // which will do the job.
    final ResultSet rs = Objects.requireNonNull(stmt, "stmt").executeQuery();
    long size; // row count of stmt
    try {
      rs.last();
      size = rs.getRow();
      rs.beforeFirst();
    } catch (Exception ex) {
      size = Long.MAX_VALUE; // row count is unknown
    }
    return StreamSupport.stream(new ResultSetSpliterator<T>(rs, size, mapper), parallel);
  }
}
