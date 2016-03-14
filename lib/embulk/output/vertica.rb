require 'jvertica'
require_relative 'vertica/value_converter_factory'
require_relative 'vertica/output_thread'

module Embulk
  module Output
    class Vertica < OutputPlugin
      Plugin.register_output('vertica', self)

      class Error < StandardError; end
      class NotSupportedType < Error; end

      def self.thread_pool
        @thread_pool ||= @thread_pool_proc.call
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
          'compress'         => config.param('compress',         :string,  :default => 'UNCOMPRESSED'),
          'default_timezone' => config.param('default_timezone', :string, :default => 'UTC'),
          'column_options'   => config.param('column_options',   :hash,    :default => {}),
          'json_payload'     => config.param('json_payload',     :bool,    :default => false),
          'resource_pool'    => config.param('resource_pool',    :string,  :default => nil),
          'reject_on_materialized_type_error' => config.param('reject_on_materialized_type_error', :bool, :default => false),
          'pool'             => config.param('pool',             :integer, :default => processor_count),
          'write_timeout'    => config.param('write_timeout',    :integer, :default => nil), # like 11 * 60 sec
          'dequeue_timeout'  => config.param('dequeue_timeout',  :integer, :default => nil), # like 13 * 60 sec
          'finish_timeout'   => config.param('finish_timeout',   :integer, :default => nil), # like 3 * 60 sec
        }

        @thread_pool_proc = Proc.new do
          OutputThreadPool.new(task, schema, task['pool'])
        end

        task['user'] ||= task['username']
        unless task['user']
          raise ConfigError.new 'required field "user" is not set'
        end

        task['mode'] = task['mode'].upcase
        unless %w[INSERT REPLACE DROP_INSERT].include?(task['mode'])
          raise ConfigError.new "`mode` must be one of INSERT, REPLACE, DROP_INSERT"
        end

        task['copy_mode'] = task['copy_mode'].upcase
        unless %w[AUTO DIRECT TRICKLE].include?(task['copy_mode'])
          raise ConfigError.new "`copy_mode` must be one of AUTO, DIRECT, TRICKLE"
        end

        # ToDo: Support BZIP, LZO
        task['compress'] = task['compress'].upcase
        unless %w[GZIP UNCOMPRESSED].include?(task['compress'])
          raise ConfigError.new "`compress` must be one of GZIP, UNCOMPRESSED"
        end

        now = Time.now
        unique_name = "%08x%08x" % [now.tv_sec, now.tv_nsec]
        task['temp_table'] = "#{task['table']}_LOAD_TEMP_#{unique_name}"

        quoted_schema     = ::Jvertica.quote_identifier(task['schema'])
        quoted_table      = ::Jvertica.quote_identifier(task['table'])
        quoted_temp_table = ::Jvertica.quote_identifier(task['temp_table'])

        connect(task) do |jv|
          unless task['json_payload'] # ToDo: auto table creation is not supported to json_payload mode yet
            sql_schema_table = self.sql_schema_from_embulk_schema(schema, task['column_options'])

            # create the target table
            query(jv, %[DROP TABLE IF EXISTS #{quoted_schema}.#{quoted_table}]) if task['mode'] == 'DROP_INSERT'
            query(jv, %[CREATE TABLE IF NOT EXISTS #{quoted_schema}.#{quoted_table} (#{sql_schema_table})])
          end

          # create a temp table
          query(jv, %[DROP TABLE IF EXISTS #{quoted_schema}.#{quoted_temp_table}])

          if task['mode'] == 'REPLACE'
            # In the case of replace mode, this temp table is replaced with the original table. So, projections should also be copied
            query(jv, %[CREATE TABLE #{quoted_schema}.#{quoted_temp_table} LIKE #{quoted_schema}.#{quoted_table} INCLUDING PROJECTIONS])
          else
            query(jv, %[CREATE TABLE #{quoted_schema}.#{quoted_temp_table} LIKE #{quoted_schema}.#{quoted_table}])
            # Create internal vertica projection beforehand, otherwirse, parallel copies lock table to create a projection and we get S Lock error sometimes
            # This is a trick to create internal vertica projection
            query(jv, %[INSERT INTO #{quoted_schema}.#{quoted_temp_table} SELECT * FROM #{quoted_schema}.#{quoted_table} LIMIT 0])
          end
          Embulk.logger.trace {
            result = query(jv, %[SELECT EXPORT_OBJECTS('', '#{task['schema']}.#{task['temp_table']}')])
            # You can see `CREATE PROJECTION` if the table has a projection
            "embulk-output-vertica: #{result.to_a.flatten}"
          }
        end

        begin
          # insert data into the temp table
          thread_pool.start
          yield(task)
          task_reports = thread_pool.commit
          Embulk.logger.info { "embulk-output-vertica: task_reports: #{task_reports.to_json}" }

          connect(task) do |jv|
            if task['mode'] == 'REPLACE'
              # swap table and drop the old table
              quoted_old_table = ::Jvertica.quote_identifier("#{task['table']}_LOAD_OLD_#{unique_name}")
              from = "#{quoted_schema}.#{quoted_table},#{quoted_schema}.#{quoted_temp_table}"
              to   = "#{quoted_old_table},#{quoted_table}"
              query(jv, %[ALTER TABLE #{from} RENAME TO #{to}])
              query(jv, %[DROP TABLE #{quoted_schema}.#{quoted_old_table}])
            else
              # insert select from the temp table
              hint = '/*+ direct */ ' if task['copy_mode'] == 'DIRECT' # I did not prepare a specific option, does anyone want?
              query(jv, %[INSERT #{hint}INTO #{quoted_schema}.#{quoted_table} SELECT * FROM #{quoted_schema}.#{quoted_temp_table}])
              jv.commit
            end
          end
        ensure
          connect(task) do |jv|
            # clean up the temp table
            query(jv, %[DROP TABLE IF EXISTS #{quoted_schema}.#{quoted_temp_table}])
            Embulk.logger.trace { "embulk-output-vertica: select result\n#{query(jv, %[SELECT * FROM #{quoted_schema}.#{quoted_table} LIMIT 10]).map {|row| row.to_h }.join("\n") rescue nil}" }
          end
        end
        # this is for -o next_config option, add some paramters for next time execution if wants
        next_config_diff = {}
        return next_config_diff
      end

      # instance is created on each thread
      def initialize(task, schema, index)
        super
      end

      # called for each page in each thread
      def close
      end

      # called for each page in each thread
      def add(page)
        self.class.thread_pool.enqueue(page)
      end

      def finish
      end

      def abort
      end

      # called after processing all pages in each thread
      # we do commit on #transaction for all pools, not at here
      def commit
        {}
      end

      private

      def self.connect(task)
        jv = ::Jvertica.connect({
          host: task['host'],
          port: task['port'],
          user: task['user'],
          password: task['password'],
          database: task['database'],
        })

        if resource_pool = task['resource_pool']
          query(jv, "SET SESSION RESOURCE_POOL = #{::Jvertica.quote(resource_pool)}")
        end

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

      def self.query(conn, sql)
        Embulk.logger.info "embulk-output-vertica: #{sql}"
        conn.query(sql)
      end

      def query(conn, sql)
        self.class.query(conn, sql)
      end
    end
  end
end
