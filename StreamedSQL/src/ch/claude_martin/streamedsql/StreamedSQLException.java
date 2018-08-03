package ch.claude_martin.streamedsql;

/** Checked exceptions that occur during streaming wrapped as an unchecked exception. */
public class StreamedSQLException extends RuntimeException {
  private static final long serialVersionUID = 6924902594003258547L;

  public StreamedSQLException(Exception cause) {
    super(cause);
  }
}
