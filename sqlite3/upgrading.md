# Upgrading sqlite

We need to make our own amalgamation, since we want to enable `SQLITE_ENABLE_UPDATE_DELETE_LIMIT` during the parser generator phase.

`-DSQLITE_ENABLE_UPDATE_DELETE_LIMIT=1` seems to be the only important option when creating the amalgamation.

```sh
# Download the canonical source (not the prebuilt amalgamation) from
# https://sqlite.org/download.html  (look for sqlite-src-XXXXXXX.zip)
wget https://sqlite.org/2026/sqlite-src-3530300.zip
unzip sqlite-src-3530300.zip
cd sqlite-src-3530300

# The configure script now uses Autosetup (no autoconf required).
# Pass the flag via CFLAGS so it propagates into the generated parser.
CFLAGS='-DSQLITE_ENABLE_UPDATE_DELETE_LIMIT=1' ./configure

# Build only the amalgamation (much faster than a full build)
make sqlite3.c

# Copy the generated files into the project
cp sqlite3.c sqlite3.h /path/to/go-sqlite-lite/sqlite3/
```

After copying, verify:

```sh
grep SQLITE_VERSION sqlite3/sqlite3.h      # should show the new version
grep -c SQLITE_ENABLE_UPDATE_DELETE_LIMIT sqlite3/sqlite3.c  # should be > 0
go test ./sqlite3/ ./slite/
```
