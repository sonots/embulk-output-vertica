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
- **abort_on_error**: Stops the COPY command if a row is rejected and rolls back the command. No data is loaded. (bool, default: false)
- **column_options**: advanced: a key-value pairs where key is a column name and value is options for the column.
  - **type**: type of a column when this plugin creates new tables such as `VARCHAR(255)`, `INTEGER NOT NULL UNIQUE`. This is used on creating intermediate tables (insert and truncate_insert modes) and on creating a new target table. (string, default: depends on input column type, see below)
    - `INT` (same with `BIGINT` in vertica) for `long`
    - `BOOLEAN` for `boolean`
    - `FLOAT` (same with `DOUBLE PRECISION` in vertica) for `double`
    - `VARCHAR` for `string`
    - `TIMESTAMP` for `timestamp`

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
  abort_on_error: true
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

## ChangeLog

[CHANGELOG.md](CHANGELOG.md)
