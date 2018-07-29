package ch.claude_martin.streamdsql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Comparator;
import java.util.stream.Stream;

/**
 * This is just a silly demonstration.
 * 
 * This might seem slow, but that's mostly the creation of the in-memory
 * database.
 * 
 * Note: You must add <a href="https://db.apache.org/derby/">Derby</a> to the
 * class/module path for this to work.
 * 
 * @author Claude Martin
 *
 */
public class Example {
  public static void main(String[] args) throws SQLException, InterruptedException {
    var conn = initDB();
    try (Stream<Foo> stream = StreamedSQL.stream(conn, "SELECT * FROM FOO WHERE NAME LIKE 'L%'", Foo::of)) {
      stream.filter(f -> f.getId() % 31 == 6).sorted(Comparator.comparing(Foo::getName)).forEach(System.out::println);
    }
    System.out.println("THE END");
  }

  /** Object representation of the Data. */
  public static class Foo {
    private final int id;
    private final String name;

    public static Foo of(ResultSet rs) throws SQLException {
      return new Foo(rs.getInt(1), rs.getString(2));
    }

    public Foo(int id, String name) {
      this.id = id;
      this.name = name;
    }

    public int getId() {
      return id;
    }

    public String getName() {
      return name;
    }

    @Override
    public String toString() {
      return String.format("(%d, %s)", this.id, this.name);
    }
  }

  private static Connection initDB() throws SQLException {
    try {
      Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
    } catch (ClassNotFoundException ex) {
      ex.printStackTrace();
    }
    Connection conn = DriverManager.getConnection("jdbc:derby:memory:TEST;create=true");
    Statement stmt = conn.createStatement();
    stmt.execute("CREATE TABLE FOO ( ID INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY, NAME VARCHAR(100) )");
    PreparedStatement ps = conn.prepareStatement("INSERT INTO FOO (NAME) VALUES (?)");
    for (String name : Arrays.asList("Fiona", "Max", "Carla", "Bert", "Lisa", "Luke", "Xenia", "Rick", "Natalie")) {
      ps.setString(1, name);
      ps.execute();
    }

    // Generate some silly names:
    final int[] cons = "qwrtzpsdfghjklxbnm".chars().toArray();
    final int[] voc = "euioa".chars().toArray();
    final StringBuilder sb = new StringBuilder();
    for (int c : cons) {
      sb.append(Character.toUpperCase((char) c));
      for (int v : voc) {
        sb.append((char) v);
        for (int c2 : cons) {
          sb.append((char) c2);
          for (int v2 : voc) {
            sb.append((char) v2);
            ps.setString(1, sb.toString());
            ps.execute();
            sb.setLength(sb.length() - 1);
          }
          sb.setLength(sb.length() - 1);
        }
        sb.setLength(sb.length() - 1);
      }
      sb.setLength(sb.length() - 1);
    }
    return conn;
  }
}
