require 'jvertica'

module Embulk
  module Output
    class Vertica < OutputPlugin
      Plugin.register_output('vertica', self)

      class Error < StandardError; end
      class NotSupportedType < Error; end

      def self.transaction(config, schema, processor_count, &control)
        task = {
          'host'           => config.param('host',           :string,  :default => 'localhost'),
          'port'           => config.param('port',           :integer, :default => 5433),
          'username'       => config.param('username',       :string),
          'password'       => config.param('password',       :string,  :default => ''),
          'database'       => config.param('database',       :string,  :default => 'vdb'),
          'schema'         => config.param('schema',         :string,  :default => 'public'),
          'table'          => config.param('table',          :string),
          'copy_mode'      => config.param('copy_mode',      :string,  :default => 'AUTO'),
          'abort_on_error' => config.param('abort_on_error', :bool,    :default => false),
          'column_options' => config.param('column_options', :hash,    :default => {}),
        }

        unless %w[AUTO DIRECT TRICKLE].include?(task['copy_mode'].upcase)
          raise ConfigError, "`copy_mode` must be one of AUTO, DIRECT, TRICKLE"
        end

        now = Time.now
        unique_name = "%08x%08x" % [now.tv_sec, now.tv_nsec]
        task['temp_table'] = "#{task['table']}_LOAD_TEMP_#{unique_name}"

        sql_schema = self.to_sql_schema(schema, task['column_options'])

        quoted_schema     = ::Jvertica.quote_identifier(task['schema'])
        quoted_table      = ::Jvertica.quote_identifier(task['table'])
        quoted_temp_table = ::Jvertica.quote_identifier(task['temp_table'])

        connect(task) do |jv|
          # drop table if exists "DEST"
          # 'create table if exists "TEMP" ("COL" json)'
          jv.query %[drop table if exists #{quoted_schema}.#{quoted_temp_table}]
          jv.query %[create table #{quoted_schema}.#{quoted_temp_table} (#{sql_schema})]
        end

        begin
          yield(task)
          connect(task) do |jv|
            # create table if not exists "DEST" ("COL" json)
            # 'insert into "DEST" ("COL") select "COL" from "TEMP"'
            jv.query %[create table if not exists #{quoted_schema}.#{quoted_table} (#{sql_schema})]
            jv.query %[insert into #{quoted_schema}.#{quoted_table} select * from #{quoted_schema}.#{quoted_temp_table}]
            jv.commit
          end
        ensure
          connect(task) do |jv|
            # 'drop table if exists TEMP'
            jv.query %[drop table if exists #{quoted_schema}.#{quoted_temp_table}]
          end
        end
        return {}
      end

      def self.connect(task)
        jv = ::Jvertica.connect({
          host: task['host'],
          port: task['port'],
          user: task['username'],
          password: task['password'],
          database: task['database'],
        })

        if block_given?
          begin
            yield jv
          ensure
            jv.close
          end
        end
        jv
      end

      # @param [Schema] schema embulk defined column types
      # @param [Hash]   column_options user defined column types
      # @return [String] sql schema used to CREATE TABLE
      def self.to_sql_schema(schema, column_options)
        schema.names.zip(schema.types).map do |column_name, type|
          sql_type = (column_options[column_name] and column_options[column_name]['type']) ?
            column_options[column_name]['type'] : to_sql_type(type)
          "#{::Jvertica.quote_identifier(column_name)} #{sql_type}"
        end.join(',')
      end

      def self.to_sql_type(type)
        case type
        when :boolean then 'BOOLEAN'
        when :long then 'INT' # BIGINT is a synonym for INT in vertica
        when :double then 'FLOAT' # DOUBLE PRECISION is a synonym for FLOAT in vertica
        when :string then 'VARCHAR' # LONG VARCHAR is not recommended
        when :timestamp then 'TIMESTAMP'
        else raise NotSupportedType, "embulk-output-vertica cannot take column type #{type}"
        end
      end

      def initialize(task, schema, index)
        super
        @jv = self.class.connect(task)
      end

      def close
        @jv.close
      end

      def add(page)
        @jv.copy(copy_sql) do |stdin|
          page.each_with_index do |record, idx|
            stdin << record.map {|v| ::Jvertica.quote(v) }.join(",") << "\n"
          end
        end
        @jv.commit
      end

      def finish
      end

      def abort
      end

      def commit
        {}
      end

      private

      def copy_sql
        quoted_schema     = ::Jvertica.quote_identifier(@task['schema'])
        quoted_temp_table = ::Jvertica.quote_identifier(@task['temp_table'])
        copy_mode         = @task['copy_mode']
        abort_on_error    = @task['abort_on_error'] ? ' ABORT ON ERROR' : ''
        sql = "COPY #{quoted_schema}.#{quoted_temp_table} FROM STDIN DELIMITER ',' #{copy_mode}#{abort_on_error} NO COMMIT"
        Embulk.logger.debug sql
        sql
      end
    end
  end
end
