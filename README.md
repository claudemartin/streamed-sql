# streamed-sql
Use Java Stream API for your JDBC queries.

It's very easy to use:

```JAVA
Connection conn = dbpool.getConnection();
try (Stream<Foo> stream = StreamedSQL.stream(conn, "SELECT * FROM FOO", Foo::new)) {
  stream.filter(f -> f.getName().startsWith("L")).sorted().forEach(System.out::println);
}
```

You get a parallel stream and you can just map the data to any POJO and then process them in parallel. 

This is based on an idea I found on Stackoverflow: https://stackoverflow.com/a/32232173/2123025

