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
- **mode**:     "insert", or "replace". See bellow. (string, default: insert)
- **copy_mode**: specifies how data is loaded into the database. (`AUTO`, `DIRECT`, or `TRICKLE`. default: AUTO) See vertica documents for details.
- **abort_on_error**: Stops the COPY command if a row is rejected and rolls back the command. No data is loaded. (bool, default: false)
- **reject_on_materialized_type_error**: Use `reject_on_materialized_type_error` option for fjsonparser(). This rejects rows if any of olumn types and value types do not fit. ex) double value into INT column fails. See vertica documents for details. (bool, default: false)
- **default_timezone**: the default timezone for column_options (string, default is "UTC")
- **column_options**: advanced: a key-value pairs where key is a column name and value is options for the column.
  - **type**: type of a column when this plugin creates new tables such as `VARCHAR(255)`, `INTEGER NOT NULL UNIQUE`. This is used on creating intermediate tables (insert and truncate_insert modes) and on creating a new target table. (string, default: depends on input column type, see below)
    - boolean:   `BOOLEAN`
    - long:      `INT` (same with `BIGINT` in vertica)
    - double:    `FLOAT` (same with `DOUBLE PRECISION` in vertica)
    - string:    `VARCHAR`
    - timestamp: `TIMESTAMP`
  - **value_type**:  The types (embulk types) of values to convert (string, default: no conversion. See below for available types)
    - boolean:   `boolean`, `string` (to\_s)
    - long:      `boolean` (true), `long`, `double` (to\_f), `string` (to\_s), `timestamp` (Time.at)
    - double:    `boolean` (true), `long` (to\_i), `double`, `string` (to\_s), `timestamp` (Time.at)
    - string:    `boolean` (true), `long` (to\_i), `double` (to\_f), `string`, `timestamp` (Time.strptime)
    - timestamp: `boolean` (true), `long` (to\_i), `double` (to\_f), `string` (strftime), `timestamp`
  - **timestamp_format**: timestamp format to convert into/from `timestamp` (string, default is "%Y-%m-%d %H:%M:%S %z")
  - **timezone**: timezone to convert into/from `timestamp` (string, default is `default_timezone`).

### Modes

* **insert**:
  * Behavior: This mode copys rows to some intermediate tables first. If all those tasks run correctly, runs INSERT INTO <target_table> SELECT * FROM <intermediate_table>
  * Transactional: Yes if `abort_on_error` option is used
* **replace**:
  * Behavior: Same with insert mode excepting that it drop the target table first.
  * Transactional: Yes if `abort_on_error` option is used

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
    id:   {type: INT}
    name: {type: VARCHAR(255)}
    date: {type: DATE, value_type: timezone, timezone: "+09:00"}
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
