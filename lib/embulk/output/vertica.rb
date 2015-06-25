require 'jvertica'

module Embulk
  module Output
    class Vertica < OutputPlugin
      Plugin.register_output('vertica', self)

      class Error < StandardError; end
      class NotSupportedType < Error; end

      def self.transaction(config, schema, processor_count, &control)
        task = {
          'host'      => config.param('host',      :string,  :default => 'localhost'),
          'port'      => config.param('port',      :integer, :default => 5433),
          'username'  => config.param('username',  :string),
          'password'  => config.param('password',  :string,  :default => ''),
          'database'  => config.param('database',  :string,  :default => 'vdb'),
          'schema'    => config.param('schema',    :string,  :default => 'public'),
          'table'     => config.param('table',     :string),
          'copy_mode' => config.param('copy_mode', :string, :default => 'AUTO'),
        }

        unless %w[AUTO DIRECT TRICKLE].include?(task['copy_mode'].upcase)
          raise ConfigError, "`copy_mode` must be one of AUTO, DIRECT, TRICKLE"
        end

        now = Time.now
        unique_name = "%08x%08x" % [now.tv_sec, now.tv_nsec]
        task['temp_table'] = "#{task['table']}_LOAD_TEMP_#{unique_name}"

        sql_schema = self.to_vertica_schema schema

        connect(task) do |jv|
          # drop table if exists "DEST"
          # 'create table if exists "TEMP" ("COL" json)'
          jv.query %[drop table if exists #{task['schema']}.#{task['temp_table']}]
          jv.query %[create table #{task['schema']}.#{task['temp_table']} (#{sql_schema})]
        end

        begin
          yield(task)
          connect(task) do |jv|
            # create table if not exists "DEST" ("COL" json)
            # 'insert into "DEST" ("COL") select "COL" from "TEMP"'
            jv.query %[create table if not exists #{task['schema']}.#{task['table']} (#{sql_schema})]
            jv.query %[insert into #{task['schema']}.#{task['table']} select * from #{task['schema']}.#{task['temp_table']}]
            jv.commit
          end
        ensure
          connect(task) do |jv|
            # 'drop table if exists TEMP'
            jv.query %[drop table if exists #{task['schema']}.#{task['temp_table']}]
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

      def self.to_vertica_schema(schema)
        schema.names.zip(schema.types)
          .map { |name, type| "#{name} #{to_sql_type(type)}" }
          .join(',')
      end

      def self.to_sql_type(type)
        case type
        when :boolean then 'BOOLEAN'
        when :long then 'INT'
        when :double then 'FLOAT'
        when :string then 'VARCHAR'
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
        sql = "COPY #{@task['schema']}.#{@task['temp_table']} FROM STDIN DELIMITER ',' #{@task['copy_mode']} NO COMMIT"
        Embulk.logger.debug sql
        @jv.copy(sql) do |stdin|
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
    end
  end
end
