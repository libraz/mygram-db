# Client search examples

Both examples connect to a running MygramDB server, issue a typed search,
iterate over every returned primary key, and release owned resources.

Build them against an installed client SDK:

```sh
cmake -S . -B build -DCMAKE_PREFIX_PATH=/path/to/mygramdb-prefix
cmake --build build
```

Run either client with the same arguments:

```sh
./build/mygramdb_cpp_search 127.0.0.1 11016 app.articles "search text"
./build/mygramdb_c_search 127.0.0.1 11016 app.articles "search text"
```
