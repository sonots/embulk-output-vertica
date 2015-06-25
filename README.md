# Vertica output plugin for Embulk

## Overview

* **Plugin type**: output
* **Load all or nothing**: no
* **Resume supported**: no
* **Cleanup supported**: yes

## Configuration

- **host**: hostname (string, default: localhost)
- **port**: port number (integer, default: 5433)
- **username**: username (string, required)
- **password**: password (string, default: '')
- **database**: database name (string, default: vdb)
- **schema**:   schema name (string, default: public)
- **table**:    table name (string, required)

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
```


## Development

Run example:

```
$ embulk gem install embulk-input-random
$ embulk gem install jvertica
$ embulk run -I lib example.yml
```

Release gem:

```
$ bundle exec rake release
```
