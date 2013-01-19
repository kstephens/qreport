require 'qreport/report_run'
require 'qreport/connection'
require 'digest/md5'
require 'base64'
require 'json'
require 'pp'

module Qreport
  class ReportRunner
    attr_accessor :connection, :verbose

    def run! report_run
      report_run.created_at ||=
        report_run.started_at = Time.now.utc
      name = report_run.name
      sql  = report_run.sql.strip

      arguments = report_run.arguments || { }
      error = error_1 = error_2 = nrows = nil

      Connection.current = connection

      begin
        conn.transaction do

          # Create a report row sequence:
          run "CREATE TEMPORARY SEQUENCE qr_row_seq"

          # Rewrite query to create result table rows:
          arguments = arguments.merge(:qr_run_id => conn.safe_sql("nextval('qr_row_seq')"))
          report_run.report_sql = report_sql(sql)

          # Proof query to infer base columns:
          result = run report_run.report_sql, :limit => 0, :arguments => arguments, :verbose => @verbose
          report_run.base_columns = result.columns
          result = nil
        end # transaction
      rescue ::Exception => exc
        error = error_1 = exc
        $stderr.puts "  error_1 = #{exc.inspect}"
      end

      # Construct report_table name from column names and types:
      report_table = report_run.report_table

      conn.transaction do
        # Create new ReportRun row:
        report_run.insert!
        report_run_id = report_run.id
        arguments[:qr_run_id] = report_run_id
        report_run.report_sql = report_sql(sql)
      end # transaction

      unless error
        # Run query into report table:
        begin
          conn.transaction do
            unless conn.table_exists? report_table
              run "CREATE TABLE #{report_table} AS #{report_run.report_sql}", :arguments => arguments, :verbose => @verbose
              run "CREATE INDEX #{report_table}_i1 ON #{report_table} (qr_run_id)"
              run "CREATE INDEX #{report_table}_i2 ON #{report_table} (qr_run_row)"
              run "CREATE UNIQUE INDEX #{report_table}_i3 ON #{report_table} (qr_run_id, qr_run_row)"
              report_run.additional_columns.each do | n, t, d |
                run "ALTER TABLE #{report_table} ADD COLUMN #{conn.escape_identifier(n)} #{t} DEFAULT :d", :arguments => { :d => d || nil }
              end
            else
              result =
              run "INSERT INTO #{report_table} #{report_run.report_sql}", :arguments => arguments, :verbose => @verbose

              # Get the number of report run rows from cmd_status:
              unless cs = result.cmd_status and cs[0] == 'INSERT' and cs[1] == 0 and nrows = cs[2]
                raise Error, "cannot determine nrows"
              end
            end
            # Get the number of report run rows:
            unless nrows || error
              result = report_run._select :COLUMNS => 'COUNT(*) AS "nrows"' #, :verbose => true
              nrows = result.rows[0]["nrows"] || (raise Error, "cannot determine nrows")
            end
            # pp result
            result = nil
          end # transaction
        rescue ::Exception => exc
          error = error_2 = exc
          $stderr.puts "  error_2 = #{exc.inspect}"
        end # transaction
      end

      conn.transaction do
        run "DROP SEQUENCE qr_row_seq" unless error_1

        # Update stats:
        report_run.finished_at = Time.now.utc
        report_run.nrows = nrows.to_i
        report_run.error = error
        report_run.update! :values => [ :nrows, :finished_at, :error ] # , :verbose_result => true
      end # transaction

      report_run
    end

    def report_sql sql
      sql = sql.sub(/\ASELECT\s+/im, <<"END"
SELECT
    :qr_run_id
       AS "qr_run_id"
  , nextval('qr_row_seq')
       AS "qr_run_row"
  , 
END
          )
      sql
    end

    def run *args
      # conn.verbose = true
      conn.run *args
    end

    def connection
      @connection ||= Connection.new
    end
    alias :conn :connection
  end
end
