require 'jvertica'
require_relative 'vertica/value_converter_factory'

module Embulk
  module Output
    class Vertica < OutputPlugin
      Plugin.register_output('vertica', self)

      class Error < StandardError; end
      class NotSupportedType < Error; end

      def self.transaction(config, schema, processor_count, &control)
        task = {
          'host'             => config.param('host',           :string,  :default => 'localhost'),
          'port'             => config.param('port',           :integer, :default => 5433),
          'username'         => config.param('username',       :string),
          'password'         => config.param('password',       :string,  :default => ''),
          'database'         => config.param('database',       :string,  :default => 'vdb'),
          'schema'           => config.param('schema',         :string,  :default => 'public'),
          'table'            => config.param('table',          :string),
          'mode'             => config.param('mode',           :string,  :default => 'insert'),
          'copy_mode'        => config.param('copy_mode',      :string,  :default => 'AUTO'),
          'abort_on_error'   => config.param('abort_on_error', :bool,    :default => false),
          'default_timezone' => config.param('default_timezone', :string, :default => 'UTC'),
          'column_options'   => config.param('column_options', :hash,    :default => {}),
          'reject_on_materialized_type_error' => config.param('reject_on_materialized_type_error', :bool, :default => false),
        }

        unless %w[INSERT REPLACE].include?(task['mode'].upcase!)
          raise ConfigError, "`mode` must be one of INSERT, REPLACE"
        end

        unless %w[AUTO DIRECT TRICKLE].include?(task['copy_mode'].upcase!)
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
          if task['mode'] == 'REPLACE'
            query(jv, %[DROP TABLE IF EXISTS #{quoted_schema}.#{quoted_table}])
          end
          query(jv, %[DROP TABLE IF EXISTS #{quoted_schema}.#{quoted_temp_table}])
          query(jv, %[CREATE TABLE #{quoted_schema}.#{quoted_temp_table} (#{sql_schema})])
        end

        begin
          yield(task)
          connect(task) do |jv|
            query(jv, %[CREATE TABLE IF NOT EXISTS #{quoted_schema}.#{quoted_table} (#{sql_schema})])
            query(jv, %[INSERT INTO #{quoted_schema}.#{quoted_table} SELECT * FROM #{quoted_schema}.#{quoted_temp_table}])
            jv.commit
          end
        ensure
          connect(task) do |jv|
            query(jv, %[DROP TABLE IF EXISTS #{quoted_schema}.#{quoted_temp_table}])
            Embulk.logger.debug { "embulk-output-vertica: #{query(jv, %[SELECT * FROM #{quoted_schema}.#{quoted_table} LIMIT 10]).map {|row| row.to_h }.join("\n") rescue nil}" }
          end
        end
        return {}
      end

      def initialize(task, schema, index)
        super
        @converters = ValueConverterFactory.create_converters(schema, task['default_timezone'], task['column_options'])
        Embulk.logger.debug { @converters.to_s }
        @jv = self.class.connect(task)
      end

      def close
        @jv.close
      end

      def add(page)
        json = nil # for log
        begin
          copy(@jv, copy_sql) do |stdin|
            page.each do |record|
              json = to_json(record)
              Embulk.logger.debug { "embulk-output-vertica: #{json}" }
              stdin << json << "\n"
            end
          end
          Embulk.logger.info "embulk-output-vertica: COMMIT!"
          @jv.commit
        rescue java.sql.SQLDataException => e
          @jv.rollback
          if @task['reject_on_materialized_type_error'] and e.message =~ /Rejected by user-defined parser/
            Embulk.logger.warn "embulk-output-vertica: ROLLBACK! some of column types and values types do not fit #{json}"
          else
            Embulk.logger.warn "embulk-output-vertica: ROLLBACK!"
          end
          raise e
        end
      end

      def finish
      end

      def abort
      end

      def commit
        {}
      end

      private

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
          if column_options[column_name] and column_options[column_name]['type']
            sql_type = column_options[column_name]['type']
          else
            sql_type = to_sql_type(type)
          end
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

      def self.query(conn, sql)
        Embulk.logger.info "embulk-output-vertica: #{sql}"
        conn.query(sql)
      end

      def query(conn, sql)
        self.class.query(conn, sql)
      end

      def copy(conn, sql, &block)
        Embulk.logger.info "embulk-output-vertica: #{sql}"
        conn.copy(sql, &block)
      end

      def copy_sql
        @copy_sql ||= "COPY #{quoted_schema}.#{quoted_temp_table} FROM STDIN#{fjsonparser}#{copy_mode}#{abort_on_error} NO COMMIT"
      end

      def to_json(record)
        Hash[*(schema.names.zip(record).map do |column_name, value|
          [column_name, @converters[column_name].call(value)]
        end.flatten!(1))].to_json
      end

      def quoted_schema
        ::Jvertica.quote_identifier(@task['schema'])
      end

      def quoted_table
        ::Jvertica.quote_identifier(@task['table'])
      end

      def quoted_temp_table
        ::Jvertica.quote_identifier(@task['temp_table'])
      end

      def copy_mode
        " #{@task['copy_mode']}"
      end

      def abort_on_error
        @task['abort_on_error'] ? ' ABORT ON ERROR' : ''
      end

      def fjsonparser
        " PARSER fjsonparser(#{reject_on_materialized_type_error})"
      end

      def reject_on_materialized_type_error
        @task['reject_on_materialized_type_error'] ? 'reject_on_materialized_type_error=true' : ''
      end
    end
  end
end
