# streamed-sql
Use Java Stream API for your JDBC queries.

It's very easy to use:

```JAVA
Connection conn = dbpool.getConnection();
final var strsql = StreamedSQL.create(conn, true); 
try (Stream<Foo> stream = strsql.stream("SELECT * FROM FOO", Foo::new)) {
  stream.filter(f -> f.getName().startsWith("L")).sorted().forEach(System.out::println);
}
```

You get a stream and you can just map the data to any POJO and then process them as a stream.   
In this example strsql is set to use a default connection and return parallel streams.

See [Example.java](https://github.com/claudemartin/streamed-sql/blob/master/StreamedSQL/src/ch/claude_martin/streamedsql/Example.java) for more details and read the javadoc.

This is based on an idea I found on Stackoverflow: https://stackoverflow.com/a/32232173/2123025

