module Embulk
  module Output
    class Vertica < OutputPlugin
      class ValueConverterFactory
        attr_reader :schema_type, :value_type, :timestamp_format, :timezone

        DEFAULT_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S %z"
        DEFAULT_TIMEZONE         = "+00:00"

        def self.create_converters(schema, column_options)
          # @param [Schema] schema embulk defined column types
          # @param [Hash]   column_options user defined column types
          # @return [Array] value converters (array of Proc)
          Hash[*(schema.names.zip(schema.types).map do |column_name, schema_type|
            if column_options[column_name]
              value_type       = column_options[column_name]['value_type']
              timestamp_format = column_options[column_name]['timestamp_format']
              timezone         = column_options[column_name]['timezone']
              [column_name, self.new(schema_type, value_type, timestamp_format, timezone).create_converter]
            else
              [column_name, Proc.new {|val| val }]
            end
          end.flatten!(1))]
        end

        def initialize(schema_type, value_type = nil, timestamp_format = nil, timezone = nil)
          @schema_type = schema_type
          @value_type = value_type || schema_type.to_s
          @timestampt_format = timestamp_format || DEFAULT_TIMESTAMP_FORMAT
          @timezone = timezone || DEFAULT_TIMEZONE
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
          when 'timestamp'  then Proc.new {|val| Time.at(val).localtime(timezone) }
          else raise NotSupportedType, "embulk-output-vertica cannot take column value_type #{value_type} for long column"
          end
        end

        def double_converter
          case value_type
          when 'boolean'   then Proc.new {|val| !!val }
          when 'long'      then Proc.new {|val| val.to_i }
          when 'double'    then Proc.new {|val| val }
          when 'string'    then Proc.new {|val| val.to_s }
          when 'timestamp' then Proc.new {|val| Time.at(val).localtime(timezone) }
          else raise NotSupportedType, "embulk-output-vertica cannot take column value_type #{value_type} for double column"
          end
        end

        def string_converter
          case value_type
          when 'boolean'   then Proc.new {|val| !!val }
          when 'long'      then Proc.new {|val| val.to_i }
          when 'double'    then Proc.new {|val| val.to_f }
          when 'string'    then Proc.new {|val| val }
          when 'timestamp' then Proc.new {|val| Time.strptime(val, timestamp_format) } # ToDo: timezone
          else raise NotSupportedType, "embulk-output-vertica cannot take column value_type #{value_type} for string column"
          end
        end

        def timestamp_converter
          case value_type
          when 'boolean'   then Proc.new {|val| !!val }
          when 'long'      then Proc.new {|val| val.to_i }
          when 'double'    then Proc.new {|val| val.to_f }
          when 'string'    then Proc.new {|val| val.localtime(timezone).strftime(timestamp_format) }
          when 'timestamp' then Proc.new {|val| val.localtime(timezone) }
          else raise NotSupportedType, "embulk-output-vertica cannot take column value_type #{value_type} for timesatmp column"
          end
        end
      end
    end
  end
end
