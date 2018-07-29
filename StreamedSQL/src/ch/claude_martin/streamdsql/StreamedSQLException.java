package ch.claude_martin.streamdsql;

/** Checked exceptions that occur during streaming. */
public class StreamedSQLException extends RuntimeException {
  private static final long serialVersionUID = 6924902594003258547L;

  public StreamedSQLException(Exception cause) {
    super(cause);
  }
}
