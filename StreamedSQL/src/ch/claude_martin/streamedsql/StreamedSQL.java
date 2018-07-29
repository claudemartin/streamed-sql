package ch.claude_martin.streamedsql;

import java.lang.ref.Cleaner;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;
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
 * soon as possible. If you prefer to be sloppy you should at least call
 * {@link #setAutoClosePhantoms(boolean) setAutoClosePhantoms(true)} before
 * creating any streams.
 * 
 * @author Claude Martin
 *
 */
public final class StreamedSQL {
  /**
   * The cleaner used for auto-closed statemens.
   * <p>
   * All use the same Cleaner. But nut every instance of {@link StreamedSQL} has
   * this feature enabled. It's a singleton that is created the first time one is
   * used.
   */
  private static volatile Cleaner cleaner = null;

  private StreamedSQL() {
    this(false);
  }

  private volatile boolean autoClose = false;

  private StreamedSQL(final boolean autoClose) {
    this.autoClose = autoClose;
  }

  /**
   * Returns a new {@link StreamedSQL}.
   * 
   * @return a new {@link StreamedSQL}
   */
  public static StreamedSQL create() {
    return new StreamedSQL();
  }

  /**
   * Returns a new {@link StreamedSQL} with {@link #setAutoClosePhantoms(boolean)
   * auto-close} enabled.
   * 
   * @param autoClose
   *          should phantom references be closed automatically?
   * @return a new {@link StreamedSQL}
   */
  public static StreamedSQL create(final boolean autoClose) {
    return new StreamedSQL(autoClose);
  }

  /**
   * You can set this to true, so that streams created in this class will
   * automatically close, even when <code>try-with-resource</code> is not used.
   * 
   * <p>
   * Note that it is recommended to properly close all streams using
   * <code>try-with-resource</code>.
   * 
   * <p>
   * If set to <code>true</code>, all phantom reachable streams will be closed,
   * which also closes the used sql statement.
   */
  public void setAutoClosePhantoms(final boolean value) {
    autoClose = value;
  }

  /**
   * Are the used statements automatically closed?
   * 
   * @see #setAutoClosePhantoms(boolean)
   */
  public boolean getAutoClosePhantoms() {
    return autoClose;
  }

  /** Returns the singleton {@link Cleaner}. Creates it if necessary. */
  private static Cleaner getCleaner() {
    if (cleaner == null) // cleaner is volatile, so this actually works.
      synchronized (StreamedSQL.class) {
        if (cleaner == null)
          return cleaner = Cleaner.create();
      }
    return cleaner;
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
  public PreparedStatement prepareStatement(final Connection conn, final String query) throws SQLException {
    final var stmt = Objects.requireNonNull(conn, "conn").prepareStatement(Objects.requireNonNull(query, "query"),
        ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
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
  public <T> Stream<T> stream(final Connection conn, final String query, final ResultSetMapper<T> mapper)
      throws SQLException {
    Objects.requireNonNull(conn, "conn");
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
  public <T> Stream<T> stream(final PreparedStatement stmt, final ResultSetMapper<T> mapper) throws SQLException {
    Objects.requireNonNull(mapper, "mapper");
    final ResultSet rs = Objects.requireNonNull(stmt, "stmt").executeQuery();
    try {
      long size; // row count of stmt
      synchronized (rs) {
        try {
          rs.last();
          size = rs.getRow();
          rs.beforeFirst();
        } catch (Exception ex) {
          size = Long.MAX_VALUE; // row count is unknown
        }
      }
      final Stream<T> stream = StreamSupport.stream(new ResultSetSpliterator<T>(rs, size, mapper), true);
      final Runnable action = () -> {
        try {
          synchronized (rs) {
            stmt.close();
          }
        } catch (SQLException e) {
          throw new StreamedSQLException(e);
        }
      };
      stream.onClose(action); // proper close by try-with-resource
      if (autoClose)
        getCleaner().register(stream, action); // fallback after gc
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
