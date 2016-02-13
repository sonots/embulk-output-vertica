require 'zlib'

module Embulk
  module Output
    class Vertica < OutputPlugin
      class CommitError < ::StandardError; end
      class TimeoutError < ::Timeout::Error; end
      class EnqueueTimeoutError < TimeoutError; end
      class DequeueTimeoutError < TimeoutError; end
      class RollbackTimeoutError < TimeoutError; end
      class CloseTimeoutError < TimeoutError; end
      class CommitTimeoutError < TimeoutError; end
      class FinishTimeoutError < TimeoutError; end
      class WriteTimeoutError < TimeoutError; end

      ROLLBACK_TIMEOUT = 1 * 60 # sec
      CLOSE_TIMEOUT = 1 * 60 # sec
      COMMIT_TIMEOUT = 5 * 60 # sec
      FINISH_TIMEOUT = 6 * 60 # sec
      WRITE_TIMEOUT = 11 * 60 # sec
      DEQUEUE_TIMEOUT = 12 * 60 # sec
      ENQUEUE_TIMEOUT = 13 * 60 # sec
      $embulk_output_vertica_thread_dumped = false

      class OutputThreadPool
        def initialize(task, schema, size)
          @task = task
          @size = size
          @schema = schema
          @converters = ValueConverterFactory.create_converters(schema, task['default_timezone'], task['column_options'])
          @output_threads = size.times.map { OutputThread.new(task) }
          @current_index = 0
        end

        def enqueue(page)
          json_page = []
          page.each do |record|
            json_page << to_json(record)
          end
          @mutex.synchronize do
            @output_threads[@current_index].enqueue(json_page)
            @current_index = (@current_index + 1) % @size
          end
        end

        def start
          @mutex = Mutex.new
          @size.times.map {|i| @output_threads[i].start }
        end

        def commit
          Embulk.logger.debug "embulk-output-vertica: commit"
          task_reports = @mutex.synchronize do
            @size.times.map {|i| @output_threads[i].commit }
          end
          unless task_reports.all? {|task_report| task_report['success'] }
            raise CommitError, "some of output_threads failed to commit"
          end
          task_reports
        end

        def to_json(record)
          if @task['json_payload']
            record.first
          else
            Hash[*(@schema.names.zip(record).map do |column_name, value|
              [column_name, @converters[column_name].call(value)]
            end.flatten!(1))].to_json
          end
        end
      end

      class OutputThread
        def initialize(task)
          @task = task
          @queue = SizedQueue.new(1)
          @num_input_rows = 0
          @num_output_rows = 0
          @num_rejected_rows = 0
          @outer_thread = Thread.current
          @thread_active = false
          @progress_log_timer = Time.now
          @previous_num_input_rows = 0

          case task['compress']
          when 'GZIP'
            @write_proc = self.method(:write_gzip)
          else
            @write_proc = self.method(:write_uncompressed)
          end
        end

        def thread_dump
          unless $embulk_output_vertica_thread_dumped
            $embulk_output_vertica_thread_dumped = true
            Embulk.logger.debug "embulk-output-vertica: kill -3 #{$$} (Thread dump)"
            begin
              Process.kill :QUIT, $$
            rescue SignalException
            ensure
              sleep 1
            end
          end
        end

        def enqueue(json_page)
          if @thread_active and @thread.alive?
            Embulk.logger.trace { "embulk-output-vertica: enqueue" }
            Timeout.timeout(ENQUEUE_TIMEOUT, EnqueueTimeoutError) do
              @queue.push(json_page)
            end
          else
            Embulk.logger.info { "embulk-output-vertica: thread is dead, but still trying to enqueue" }
            thread_dump
            raise RuntimeError, "embulk-output-vertica: thread is died, but still trying to enqueue"
          end
        end

        def write_gzip(io, page, &block)
          buf = Zlib::Deflate.new
          write_buf(buf, page, &block)
          write_io(io, buf.finish)
        end

        def write_uncompressed(io, page, &block)
          buf = ''
          write_buf(buf, page, &block)
          write_io(io, buf)
        end

        PIPE_BUF = 4096

        def write_io(io, str)
          str = str.force_encoding('ASCII-8BIT')
          i = 0
          # split str not to be blocked (max size of pipe buf is 64k bytes on Linux, Mac at default)
          while substr = str[i, PIPE_BUF]
            Timeout.timeout(WRITE_TIMEOUT, WriteTimeoutError) { io.write(substr) }
            i += PIPE_BUF
          end
        end

        def write_buf(buf, json_page, &block)
          json_page.each do |record|
            yield(record) if block_given?
            Embulk.logger.trace { "embulk-output-vertica: record #{record}" }
            buf << record << "\n"
            @num_input_rows += 1
          end
          now = Time.now
          if @progress_log_timer < now - 10 # once in 10 seconds
            speed = ((@num_input_rows - @previous_num_input_rows) / (now - @progress_log_timer).to_f).round(1)
            @progress_log_timer = now
            @previous_num_input_rows = @num_input_rows
            Embulk.logger.info { "embulk-output-vertica: num_input_rows #{num_format(@num_input_rows)} (#{num_format(speed)} rows/sec)" }
          end
        end

        def num_format(number)
          number.to_s.gsub(/(\d)(?=(\d{3})+(?!\d))/, '\1,')
        end

        # @return [Array] dequeued json_page
        # @return [String] 'finish' is dequeued to finish
        def dequeue
          json_page = nil
          Timeout.timeout(DEQUEUE_TIMEOUT, DequeueTimeoutError) { json_page = @queue.pop }
          Embulk.logger.trace { "embulk-output-vertica: dequeued" }
          Embulk.logger.debug { "embulk-output-vertica: dequeued finish" } if json_page == 'finish'
          json_page
        end

        def copy(jv, sql, &block)
          Embulk.logger.debug "embulk-output-vertica: copy, waiting a first message"

          num_output_rows = 0; rejected_row_nums = []; last_record = nil

          json_page = dequeue
          return [num_output_rows, rejected_row_nums, last_record] if json_page == 'finish'

          Embulk.logger.debug "embulk-output-vertica: #{sql}"

          num_output_rows, rejected_row_nums = jv.copy(sql) do |stdin, stream|
            @write_proc.call(stdin, json_page) {|record| last_record = record }

            while true
              json_page = dequeue
              break if json_page == 'finish'
              @write_proc.call(stdin, json_page) {|record| last_record = record }
            end
          end

          @num_output_rows += num_output_rows
          @num_rejected_rows += rejected_row_nums.size
          Embulk.logger.info { "embulk-output-vertica: COMMIT!" }
          Timeout.timeout(COMMIT_TIMEOUT, CommitTimeoutError) { jv.commit }
          Embulk.logger.debug { "embulk-output-vertica: COMMITTED!" }

          if rejected_row_nums.size > 0
            Embulk.logger.debug { "embulk-output-vertica: rejected_row_nums: #{rejected_row_nums}" }
          end

          [num_output_rows, rejected_row_nums, last_record]
        end

        def run
          Embulk.logger.debug { "embulk-output-vertica: thread started" }
          begin
            jv = Vertica.connect(@task)
            begin
              num_output_rows, rejected_row_nums, last_record = copy(jv, copy_sql)
              Embulk.logger.debug { "embulk-output-vertica: thread finished" }
            rescue java.sql.SQLDataException => e
              if @task['reject_on_materialized_type_error'] and e.message =~ /Rejected by user-defined parser/
                Embulk.logger.warn "embulk-output-vertica: ROLLBACK! some of column types and values types do not fit #{rejected_row_nums}"
              else
                Embulk.logger.warn "embulk-output-vertica: ROLLBACK! #{rejected_row_nums}"
              end
              Embulk.logger.info { "embulk-output-vertica: last_record: #{last_record}" }
              rollback(jv)
              raise e
            rescue => e
              Embulk.logger.warn "embulk-output-vertica: ROLLBACK! #{e.class} #{e.message} #{e.backtrace.join("\n  ")}"
              rollback(jv)
              Embulk.logger.debug "embulk-output-vertica: raise e"
              raise e
            end
          ensure
            close(jv)
          end
        rescue TimeoutError => e
          Embulk.logger.error "embulk-output-vertica: UNKNOWN TIMEOUT!! #{e.class}"
          @thread_active = false # not to be enqueued any more
          while @queue.size > 0
            @queue.pop # dequeue all because some might be still trying @queue.push and get blocked, need to release
          end
          thread_dump
          exit(1)
        rescue => e
          Embulk.logger.debug "embulk-output-vertica: @thread_active = false"
          @thread_active = false # not to be enqueued any more
          Embulk.logger.debug "embulk-output-vertica: dequeue all"
          while @queue.size > 0
            @queue.pop # dequeue all because some might be still trying @queue.push and get blocked, need to release
          end
          Embulk.logger.debug "embulk-output-vertica: @outer_thread.raise"
          @outer_thread.raise e
        end

        def close(jv)
          begin
            Timeout.timeout(CLOSE_TIMEOUT, CloseTimeoutError) { jv.close }
          rescue TimeoutError => ex
            Embulk.logger.warn "embulk-output-vertica: CLOSE timeout"
          end
        end

        def rollback(jv)
          begin
            Timeout.timeout(ROLLBACK_TIMEOUT, RollbackTimeoutError) { jv.rollback }
          rescue TimeoutError => ex
            Embulk.logger.warn "embulk-output-vertica: ROLLBACK timeout"
          end
        end

        def start
          @thread = Thread.new(&method(:run))
          @thread_active = true
        end

        def commit
          Embulk.logger.debug "embulk-output-vertica: output_thread commit"
          @thread_active = false
          success = true
          if @thread.alive?
            Embulk.logger.debug { "embulk-output-vertica: push finish" }
            @queue.push('finish')
            Thread.pass
            @thread.join(FINISH_TIMEOUT)
            if @thread.alive?
              @thread.kill
              Embulk.logger.error "embulk-output-vertica: hard_limit #{FINISH_TIMEOUT}sec exceeded, thread is killed forcely"
              success = false
            end
          else
            Embulk.logger.error "embulk-output-vertica: thread died accidently"
            success = false
          end

          task_report = {
            'num_input_rows' => @num_input_rows,
            'num_output_rows' => @num_output_rows,
            'num_rejected_rows' => @num_rejected_rows,
            'success' => success
          }
        end

        # private

        def copy_sql
          @copy_sql ||= "COPY #{quoted_schema}.#{quoted_temp_table} FROM STDIN#{compress}#{fjsonparser}#{copy_mode}#{abort_on_error} NO COMMIT"
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

        def compress
          " #{@task['compress']}"
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
end
