# Vertica output plugin for Embulk

## Overview

* **Plugin type**: output
* **Resume supported**: no
* **Cleanup supported**: no
* **Dynamic table creating**: yes

## Configuration

- **host**: hostname (string, default: localhost)
- **port**: port number (integer, default: 5433)
- **username**: username (string, required)
- **password**: password (string, default: '')
- **database**: database name (string, default: vdb)
- **schema**:   schema name (string, default: public)
- **table**:    table name (string, required)
- **copy_mode**: specifies how data is loaded into the database. (`AUTO`, `DIRECT`, or `TRICKLE`. default: AUTO)
- **column_options**: advanced: a key-value pairs where key is a column name and value is options for the column.
  - **type**: type of a column when this plugin creates new tables (e.g. VARCHAR(255), INTEGER NOT NULL UNIQUE). This used when this plugin creates intermediate tables (insert and truncate_insert modes), and when it creates nonexistent target table automatically. (string, default: depends on input column type. INT (same with BIGINT in vertica) if input column type is long, BOOLEAN if boolean, FLOAT (same with DOUBLE PRECISION in vertica) if double, VARCHAR if string, TIMESTAMP if timestamp)

## Example

```yaml
out:
  type: vertica 
  host: 127.0.0.1
  username: dbadmin
  password: xxxxxxx
  database: vdb
  schema: sandbox
  table: embulk_test
  copy_mode: DIRECT
  column_options:
    id: {type: INT}
    name: {type: VARCHAR(255)}
```


## Development

Run example:

```
$ bundle install
$ bundle exec embulk run -l debug example.yml
```

Release gem:

```
$ bundle exec rake release
```
