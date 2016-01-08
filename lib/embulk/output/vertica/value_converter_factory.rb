require 'time'
require 'tzinfo'

module Embulk
  module Output
    class Vertica < OutputPlugin
      class ValueConverterFactory
        attr_reader :schema_type, :value_type, :timestamp_format, :timezone, :zone_offset

        DEFAULT_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S %z"

        # @param [Schema] schema embulk defined column types
        # @param [String] default_timezone
        # @param [Hash]   column_options user defined column types
        # @return [Hash] hash whose key is column_name, and value is its converter (Proc)
        def self.create_converters(schema, default_timezone, column_options)
          Hash[schema.names.zip(schema.types).map do |column_name, schema_type|
            if column_options[column_name]
              value_type       = column_options[column_name]['value_type']
              timestamp_format = column_options[column_name]['timestamp_format'] || DEFAULT_TIMESTAMP_FORMAT
              timezone         = column_options[column_name]['timezone'] || default_timezone
              [column_name, self.new(schema_type, value_type, timestamp_format, timezone).create_converter]
            else
              [column_name, self.new(schema_type, nil, nil, default_timezone).create_converter]
            end
          end]
        end

        def initialize(schema_type, value_type = nil, timestamp_format = nil, timezone = nil)
          @schema_type = schema_type
          @value_type = value_type || schema_type.to_s
          if @schema_type == :timestamp || @value_type == 'timestamp'
            @timestamp_format = timestamp_format
            @timezone = timezone
            @zone_offset = get_zone_offset(@timezone) if @timezone
          end
        end

        def create_converter
          case schema_type
          when :boolean   then boolean_converter
          when :long      then long_converter
          when :double    then double_converter
          when :string    then string_converter
          when :timestamp then timestamp_converter
          else raise NotSupportedType, "embulk-output-vertica cannot take column type #{schema_type}"
          end
        end

        def boolean_converter
          case value_type
          when 'boolean' then Proc.new {|val| val }
          when 'string'  then Proc.new {|val| val.to_s }
          else raise NotSupportedType, "embulk-output-vertica cannot take column value_type #{value_type} for boolean column"
          end
        end

        def long_converter
          case value_type
          when 'boolean'    then Proc.new {|val| !!val }
          when 'long'       then Proc.new {|val| val }
          when 'double'     then Proc.new {|val| val.to_f }
          when 'string'     then Proc.new {|val| val.to_s }
          when 'timestamp'  then Proc.new {|val| val ? Time.at(val).localtime(zone_offset) : nil }
          else raise NotSupportedType, "embulk-output-vertica cannot take column value_type #{value_type} for long column"
          end
        end

        def double_converter
          case value_type
          when 'boolean'   then Proc.new {|val| !!val }
          when 'long'      then Proc.new {|val| val.to_i }
          when 'double'    then Proc.new {|val| val }
          when 'string'    then Proc.new {|val| val.to_s }
          when 'timestamp' then Proc.new {|val| val ? Time.at(val).localtime(zone_offset) : nil }
          else raise NotSupportedType, "embulk-output-vertica cannot take column value_type #{value_type} for double column"
          end
        end

        def string_converter
          case value_type
          when 'boolean'   then Proc.new {|val| !!val }
          when 'long'      then Proc.new {|val| val.to_i }
          when 'double'    then Proc.new {|val| val.to_f }
          when 'string'    then Proc.new {|val| val }
          when 'timestamp' then Proc.new {|val| val ? strptime_with_zone(val, timestamp_format, zone_offset) : nil }
          else raise NotSupportedType, "embulk-output-vertica cannot take column value_type #{value_type} for string column"
          end
        end

        def timestamp_converter
          case value_type
          when 'boolean'   then Proc.new {|val| !!val }
          when 'long'      then Proc.new {|val| val.to_i }
          when 'double'    then Proc.new {|val| val.to_f }
          when 'string'    then Proc.new {|val| val ? val.localtime(zone_offset).strftime(timestamp_format) : nil }
          when 'timestamp' then Proc.new {|val| val ? val.localtime(zone_offset) : nil }
          else raise NotSupportedType, "embulk-output-vertica cannot take column value_type #{value_type} for timesatmp column"
          end
        end

        private
        
        # [+-]HH:MM, [+-]HHMM, [+-]HH
        NUMERIC_PATTERN = %r{\A[+-]\d\d(:?\d\d)?\z}

        # Region/Zone, Region/Zone/Zone
        NAME_PATTERN = %r{\A[^/]+/[^/]+(/[^/]+)?\z}

        def strptime_with_zone(date, timestamp_format, zone_offset)
          time = Time.strptime(date, timestamp_format)
          utc_offset = time.utc_offset
          time.localtime(zone_offset) + utc_offset - zone_offset
        end

        def get_zone_offset(timezone)
          if NUMERIC_PATTERN === timezone
            Time.zone_offset(timezone)
          elsif NAME_PATTERN === timezone || 'UTC' == timezone
            tz = TZInfo::Timezone.get(timezone)
            tz.period_for_utc(Time.now).utc_total_offset
          else
            raise ArgumentError, "timezone format is invalid: #{timezone}"
          end
        end
      end
    end
  end
end
