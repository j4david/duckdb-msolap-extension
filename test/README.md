# Testing this extension
This directory contains all the tests for this extension. The `sql` directory holds tests that are written as [SQLLogicTests](https://duckdb.org/dev/sqllogictest/intro.html). DuckDB aims to have most its tests in this format as SQL statements, so for the quack extension, this should probably be the goal too.

The root makefile contains targets to build and run all of these tests. To run the SQLLogicTests:
```bash
make test
```
or 
```bash
make test_debug
```

# Building

```bash
make release -e EXT_CONFIG='c:/git/hub/duckdb-msolap-extension/extension_config.cmake'
```

```bash
cd build/release && cmake --build . --config Release
```


# Sample query
```sql
from msolap('Provider=MSOLAP;Data Source=localhost:55547;Catalog=98d0040e-68a0-4a81-8402-939249ef6f6c',
  'evaluate row("Example",123)') 
```