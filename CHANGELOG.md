# 0.5.1 (2015/12/04)

Fixes:

* Need mutex.synchroize for OutputThreadPool#enqueue because it is called by #add which is ran by multiple threads

# 0.5.0 (2015/12/04)

Changes:

* Use thread pool instead of connection pool #13

# 0.4.1 (2015/12/04)

Fixes:

* Create internal vertica projection beforehand to avoid S Lock error 

# 0.4.0 (2015/11/24)

Enhancements:

* Support connection pool

# 0.3.1 (2015/11/20)

Fixes:

* Fix timezone support for the case that column_options is not specified (use default_timezone)

# 0.3.0 (2015/11/17)

Changes:

* Change log level of COMMIT statement from info to debug

# 0.2.9 (2015/11/17)

Changes:

* Change log level of COPY statement from info to debug

# 0.2.8 (2015/11/06)

Enhancements:

* Get sql schema from the existing target table to create internal temporary tables to avoid schema conflicts

# 0.2.7 (2015/11/06)

Skipped

# 0.2.6 (2015/11/06)

Fixes:

* Fix not to raise ConfigError for upcase mode, and copy_mode

# 0.2.5 (2015/10/26)

Changes:

* Output task_reports log as json

# 0.2.4 (2015/10/23)

Changes:

* Rename `username` to `user` to be compatible with ruby vertica gem and jruby jvertica gem
  * still, support `username` for backward compatibility

# 0.2.3 (2015/09/16)

Changes:

* Commit all pages at burst (in each task)

Enhancements:

* Return task_reports

# 0.2.2 (2015/07/24)

Changes:

* Change some log level from debug to info

# 0.2.1 (2015/07/24)

Fixes:

* Fix to support timezone 'UTC'

# 0.2.0 (2015/07/24)

Enhancements:

* Add `default_timezone` option

# 0.1.9 (2015/07/24)

Enhancements:

* Support `timezone` for string converter

# 0.1.8 (2015/07/24)

Enhancements:

* Support `value_type`, `timezone_format`, `timezone` option for column_options

# 0.1.7 (2015/07/24)

Enhancements:

* Add `reject_on_materialized_type_error` option

# 0.1.6 (2015/07/23)

Enhancements:

* Enhancement of debug log

# 0.1.5 (2015/07/23)

Fixes:

* Use PARSER fjsonparser() instead of DELIMITER ',', otherwise escape is too difficult to do

# 0.1.4 (2015/07/10)

Fixes:

* Just fix gemspec

# 0.1.3 (2015/07/05)

Enhancements:

* Escape schama, table names
* Add `abort_on_error` option

# 0.1.2 (2015/06/26)

Enhancements:

* Add `column_options` option

# 0.1.1 (2015/06/25)

Enhancements:

* Add `copy_mode` option
* Use `jvertica` gem instead of `vertica` gem

# 0.1.0

first version

