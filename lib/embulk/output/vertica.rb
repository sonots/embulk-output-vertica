require 'jvertica'
require 'connection_pool'
require_relative 'vertica/value_converter_factory'

module Embulk
  module Output
    class Vertica < OutputPlugin
      Plugin.register_output('vertica', self)

      class Error < StandardError; end
      class NotSupportedType < Error; end

      def self.connection_pool
        @connection_pool ||= @connection_pool_proc.call
      end

      def self.transaction(config, schema, processor_count, &control)
        task = {
          'host'             => config.param('host',             :string,  :default => 'localhost'),
          'port'             => config.param('port',             :integer, :default => 5433),
          'user'             => config.param('user',             :string,  :default => nil),
          'username'         => config.param('username',         :string,  :default => nil), # alias to :user for backward compatibility
          'password'         => config.param('password',         :string,  :default => ''),
          'database'         => config.param('database',         :string,  :default => 'vdb'),
          'schema'           => config.param('schema',           :string,  :default => 'public'),
          'table'            => config.param('table',            :string),
          'mode'             => config.param('mode',             :string,  :default => 'insert'),
          'copy_mode'        => config.param('copy_mode',        :string,  :default => 'AUTO'),
          'abort_on_error'   => config.param('abort_on_error',   :bool,    :default => false),
          'default_timezone' => config.param('default_timezone', :string,  :default => 'UTC'),
          'column_options'   => config.param('column_options',   :hash,    :default => {}),
          'reject_on_materialized_type_error' => config.param('reject_on_materialized_type_error', :bool, :default => false),
          'pool'             => config.param('pool',             :integer, :default => processor_count),
          'pool_timeout'     => config.param('pool_timeout',     :integer, :default => 600),
        }
        task['user'] ||= task['username']

        @connection_pool_proc = Proc.new do
          ConnectionPool.new(size: task['pool'], timeout: task['pool_timeout']) do
            ::Jvertica.connect({
              host: task['host'],
              port: task['port'],
              user: task['user'],
              password: task['password'],
              database: task['database'],
            })
          end
        end

        task['user'] ||= task['username']
        unless task['user']
          raise ConfigError.new 'required field "user" is not set'
        end

        task['mode'] = task['mode'].upcase
        unless %w[INSERT REPLACE].include?(task['mode'])
          raise ConfigError.new "`mode` must be one of INSERT, REPLACE"
        end

        task['copy_mode'] = task['copy_mode'].upcase
        unless %w[AUTO DIRECT TRICKLE].include?(task['copy_mode'])
          raise ConfigError.new "`copy_mode` must be one of AUTO, DIRECT, TRICKLE"
        end

        now = Time.now
        unique_name = "%08x%08x" % [now.tv_sec, now.tv_nsec]
        task['temp_table'] = "#{task['table']}_LOAD_TEMP_#{unique_name}"

        quoted_schema     = ::Jvertica.quote_identifier(task['schema'])
        quoted_table      = ::Jvertica.quote_identifier(task['table'])
        quoted_temp_table = ::Jvertica.quote_identifier(task['temp_table'])

        sql_schema_table = self.sql_schema_from_embulk_schema(schema, task['column_options'])

        # create the target table
        connection_pool.with do |jv|
          query(jv, %[DROP TABLE IF EXISTS #{quoted_schema}.#{quoted_table}]) if task['mode'] == 'REPLACE'
          query(jv, %[CREATE TABLE IF NOT EXISTS #{quoted_schema}.#{quoted_table} (#{sql_schema_table})])
        end

        sql_schema_temp_table = self.sql_schema_from_table(task)

        # create a temp table
        connection_pool.with do |jv|
          query(jv, %[DROP TABLE IF EXISTS #{quoted_schema}.#{quoted_temp_table}])
          query(jv, %[CREATE TABLE #{quoted_schema}.#{quoted_temp_table} (#{sql_schema_temp_table})])
        end

        begin
          # insert data into the temp table
          task_reports = yield(task) # obtain an array of task_reports where one report is of a task
          connection_pool.shutdown do |jv| # just don't know how to loop all connections
            jv.commit
            Embulk.logger.info { "embulk-output-vertica: COMMIT!" }
            jv.close rescue nil
          end
          @connection_pool = nil
          Embulk.logger.info { "embulk-output-vertica: task_reports: #{task_reports.to_json}" }

          # insert select from the temp table
          connection_pool.with do |jv|
            query(jv, %[INSERT INTO #{quoted_schema}.#{quoted_table} SELECT * FROM #{quoted_schema}.#{quoted_temp_table}])
            jv.commit
          end
        ensure
          connection_pool.with do |jv|
            # clean up the temp table
            Embulk.logger.debug { "embulk-output-vertica: select count #{query(jv, %[SELECT count(*) FROM #{quoted_schema}.#{quoted_temp_table}]).map {|row| row.to_h }.join("\n") rescue nil}" }
            Embulk.logger.trace { "embulk-output-vertica: select limit 10\n#{query(jv, %[SELECT * FROM #{quoted_schema}.#{quoted_temp_table} LIMIT 10]).map {|row| row.to_h }.join("\n") rescue nil}" }
            query(jv, %[DROP TABLE IF EXISTS #{quoted_schema}.#{quoted_temp_table}])
          end

          connection_pool.shutdown do |jv|
            jv.close rescue nil
          end
        end
        # this is for -o next_config option, add some paramters for next time execution if wants
        next_config_diff = {}
        return next_config_diff
      end

      def initialize(task, schema, index)
        super
        @converters = ValueConverterFactory.create_converters(schema, task['default_timezone'], task['column_options'])
        Embulk.logger.trace { @converters.to_s }
        @num_input_rows = 0
        @num_output_rows = 0
        @num_rejected_rows = 0
      end

      def connection_pool
        self.class.connection_pool
      end

      def close
        # do not close connection_pool on each thread / page
      end

      def add(page)
        connection_pool.with do |jv| # block if no available connection left
          json = nil # for log
          begin
            num_output_rows, rejects = copy(jv, copy_sql) do |stdin|
              page.each do |record|
                json = to_json(record)
                Embulk.logger.debug { "embulk-output-vertica: to_json #{json}" }
                stdin << json << "\n"
                @num_input_rows += 1
              end
            end
            num_rejected_rows = rejects.size
            @num_output_rows += num_output_rows
            @num_rejected_rows += num_rejected_rows
          rescue java.sql.SQLDataException => e
            jv.rollback
            if @task['reject_on_materialized_type_error'] and e.message =~ /Rejected by user-defined parser/
              Embulk.logger.warn "embulk-output-vertica: ROLLBACK! some of column types and values types do not fit #{json}"
            else
              Embulk.logger.warn "embulk-output-vertica: ROLLBACK!"
            end
            raise e # die transaction
          end
        end
      end

      def finish
      end

      def abort
      end

      # this is called after processing all pages in a thread
      # we do commit on #transaction for all connection pools, not at here
      def commit
        Embulk.logger.debug { "embulk-output-vertica: #{@num_output_rows} rows" }
        task_report = {
          "num_input_rows" => @num_input_rows,
          "num_output_rows" => @num_output_rows,
          "num_rejected_rows" => @num_rejected_rows,
        }
      end

      private

      # @param [Schema] schema embulk defined column types
      # @param [Hash]   column_options user defined column types
      # @return [String] sql schema used to CREATE TABLE
      def self.sql_schema_from_embulk_schema(schema, column_options)
        sql_schema = schema.names.zip(schema.types).map do |column_name, type|
          if column_options[column_name] and column_options[column_name]['type']
            sql_type = column_options[column_name]['type']
          else
            sql_type = sql_type_from_embulk_type(type)
          end
          [column_name, sql_type]
        end
        sql_schema.map {|name, type| "#{::Jvertica.quote_identifier(name)} #{type}" }.join(',')
      end

      def self.sql_type_from_embulk_type(type)
        case type
        when :boolean then 'BOOLEAN'
        when :long then 'INT' # BIGINT is a synonym for INT in vertica
        when :double then 'FLOAT' # DOUBLE PRECISION is a synonym for FLOAT in vertica
        when :string then 'VARCHAR' # LONG VARCHAR is not recommended. Default is VARCHAR(80)
        when :timestamp then 'TIMESTAMP'
        else raise NotSupportedType, "embulk-output-vertica cannot take column type #{type}"
        end
      end

      def self.sql_schema_from_table(task)
        quoted_schema = Jvertica.quote(task['schema'])
        quoted_table  = Jvertica.quote(task['table'])
        sql = "SELECT column_name, data_type FROM v_catalog.columns " \
          "WHERE table_schema = #{quoted_schema} AND table_name = #{quoted_table}"

        sql_schema = {}
        connection_pool.with do |jv|
          result = query(jv, sql)
          sql_schema = result.map {|row| [row[0], row[1]] }
        end
        sql_schema.map {|name, type| "#{::Jvertica.quote_identifier(name)} #{type}" }.join(',')
      end

      def self.query(conn, sql)
        Embulk.logger.info "embulk-output-vertica: #{sql}"
        conn.query(sql)
      end

      def query(conn, sql)
        self.class.query(conn, sql)
      end

      def copy(conn, sql, &block)
        Embulk.logger.debug "embulk-output-vertica: #{sql}"
        results, rejects = conn.copy(sql, &block)
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
