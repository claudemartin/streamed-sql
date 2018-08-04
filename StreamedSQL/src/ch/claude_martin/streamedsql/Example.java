package ch.claude_martin.streamedsql;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.Random;
import java.util.stream.Stream;

/**
 * This is just a silly demonstration.
 * 
 * <p>
 * This might seem slow, but that's mostly the creation of the in-memory
 * database.
 * 
 * <p>
 * Note: You must add <a href="https://db.apache.org/derby/">Derby</a> to the
 * class/module path for this to work.
 * 
 * @author Claude Martin
 *
 */
public class Example {
  public static void main(String[] args) throws SQLException, InterruptedException {
    try (final var conn = initDB()) {
      final var strsql = StreamedSQL.create(conn, true);
      try (Stream<Foo> stream = strsql.stream("SELECT * FROM FOO WHERE NAME LIKE 'L%' ORDER BY NAME", Foo::of)) {
        stream.filter(f -> f.getName().charAt(1) != f.getName().charAt(3))
            .filter(f -> f.getBirthdate().getDayOfWeek() == DayOfWeek.FRIDAY)
            .filter(f -> f.getBirthdate().getDayOfMonth() == 13)
            .sorted(Comparator.comparing(Foo::getBirthdate)).sequential().forEachOrdered(System.out::println);
      }
    }
    System.out.println("THE END");
  }

  /** Object representation of the data in table <i>FOO</i>. */
  public static class Foo {
    private final int id;
    private final String name;
    private final LocalDate birthdate;

    /** Maps a {@link ResultSet} to {@link Foo}. */
    public static Foo of(ResultSet rs) throws SQLException {
      return new Foo(rs.getInt(1), rs.getString(2), rs.getDate(3).toLocalDate());
    }

    public Foo(int id, String name, LocalDate birthdate) {
      this.id = id;
      this.name = name;
      this.birthdate = birthdate;
    }

    public int getId() {
      return id;
    }

    public String getName() {
      return name;
    }

    public LocalDate getBirthdate() {
      return birthdate;
    }

    @Override
    public String toString() {
      return String.format("(%d, %s, %s)", this.id, this.name, this.birthdate.format(DateTimeFormatter.ISO_DATE));
    }
  }

  /** Creates and populates a database. */
  private static Connection initDB() throws SQLException {
    try {
      Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
    } catch (ClassNotFoundException ex) {
      ex.printStackTrace();
    }
    Connection conn = DriverManager.getConnection("jdbc:derby:memory:TEST;create=true");
    Statement stmt = conn.createStatement();
    stmt.execute(
        "CREATE TABLE FOO ( ID INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY, NAME VARCHAR(100), BIRTHDATE DATE )");
    final PreparedStatement ps = conn.prepareStatement("INSERT INTO FOO (NAME, BIRTHDATE) VALUES (?, ?)");
    // Generate some silly and random names and dates of birth:
    final var rng = new Random(42L);
    final var cons = rnd("wrtzpsdfghjklxbnm", rng);
    final var voc = rnd("euioa", rng);
    final var sb = new StringBuilder();
    for (var c : cons) {
      sb.append(Character.toUpperCase((char) c));
      for (var v : voc) {
        sb.append((char) v);
        for (var c2 : cons) {
          sb.append((char) c2);
          for (var v2 : voc) {
            sb.append((char) v2);
            final String string = sb.toString();
            ps.setString(1, string);
            ps.setDate(2,
                Date.valueOf(LocalDate.of(rng.nextInt(110) + 1900, rng.nextInt(12) + 1, rng.nextInt(28) + 1)));
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

  private static int[] rnd(String str, Random rng) {
    return str.chars().mapToObj(c -> c).sorted((a, b) -> rng.nextInt(3) - 1).mapToInt(i -> i).toArray();
  }
}
