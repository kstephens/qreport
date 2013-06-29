require 'qreport'
require 'qreport/model'
require 'qreport/initialization'

module Qreport
  class ReportRun
    include Model, Initialization

    attr_accessor :id
    attr_accessor :name, :sql, :additional_columns
    attr_accessor :description
    attr_accessor :arguments
    attr_accessor :report_id
    attr_accessor :report_sql
    attr_accessor :columns, :base_columns, :column_signature
    attr_accessor :report_table
    attr_accessor :nrows
    attr_accessor :created_at, :started_at, :finished_at
    attr_accessor :verbose

    # Construct report_table name from column names and types.
    def report_table
      @report_table ||=
        "#{name}_#{column_signature}".
        gsub(/[^a-z0-9_]/i, '_').freeze
    end

    def column_signature
      @column_signature ||=
        begin
          return @column_signature = "ERROR" if base_columns.empty?
          column_signature_string = columns.to_json
          @column_signature_hash = Digest::MD5.hexdigest(column_signature_string)
          Base64.strict_encode64(Digest::MD5.digest(column_signature_string)).
            sub(/=+$/, '').
            gsub(/[^a-z0-9]/i, '_').
            downcase.
            freeze
        end
    end

    def base_columns
      @base_columns ||= EMPTY_Array
    end
    def base_columns= x
      @base_columns = x
      @columns = nil
    end

    def additional_columns
      @additional_columns ||= EMPTY_Array
    end
    def additional_columns= x
      @additional_columns ||= EMPTY_Array
      @columns = nil
    end

    def columns
      @columns ||=
        base_columns +
        additional_columns.map{|x| x.map(&:to_s)}
    end

    def run! conn
      runner = Qreport::ReportRunner.new
      runner.connection = conn
      runner.run!(self)
      self
    end

    def error
      self.error = @error if String === @error
      @error
    end

    def error= x
      case x
      when nil, Hash
        @error = x
      when String
        @error = JSON.parse(x)
      when Exception
        @error = { :error_class => x.class.name, :error_message => x.message }
      else
        raise TypeError
      end
    end

    def self.schema! conn, options = { }
      result = conn.run <<"END", options.merge(:capture_error => true) # , :verbose => true
CREATE SEQUENCE qr_report_runs_pkey;
CREATE TABLE -- IF NOT EXISTS
qr_report_runs (
    id           INTEGER PRIMARY KEY DEFAULT nextval('qr_report_runs_pkey')
  , name         VARCHAR(255) NOT NULL
  , sql          TEXT NOT NULL
  , description  TEXT NOT NULL
  , arguments    TEXT NOT NULL
  , base_columns TEXT NOT NULL
  , additional_columns TEXT NOT NULL
  , report_table VARCHAR(255) NOT NULL
  , error        TEXT
  , created_at   TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
  , started_at   TIMESTAMP WITH TIME ZONE
  , finished_at  TIMESTAMP WITH TIME ZONE
  , nrows        INTEGER
);
CREATE INDEX qr_report_runs__name ON qr_report_runs (name);
CREATE INDEX qr_report_runs__report_table ON qr_report_runs (report_table);
CREATE INDEX qr_report_runs__created_at ON qr_report_runs (created_at);
END
    end

    def insert!
      values = {
        :name => name,
        :sql => sql,
        :description => description,
        :arguments => (arguments || { }),
        :base_columns => base_columns,
        :additional_columns => additional_columns,
        :report_table => report_table,
        :error => error,
        :created_at => created_at,
        :started_at => started_at,
        :finished_at => finished_at,
        :nrows => nrows,
      }

      result = conn.run 'INSERT INTO qr_report_runs ( :NAMES ) VALUES ( :VALUES ) RETURNING id',
      :arguments => { :names_and_values => values } # , :verbose => true, :verbose_arguments => true
      self.id = result.rows[0]["id"] or raise "no id"

      self
    end

    def _options options
      options ||= EMPTY_Hash
      arguments = options[:arguments] || EMPTY_Hash
      arguments = arguments.merge(:qr_run_id => id)
      options.merge(:arguments => arguments)
    end

    def update! options = nil
      options = _options options
      values = options[:values] || EMPTY_Hash
      if Array === values
        h = { }
        values.each { | k | h[k] = send(k) }
        values = h
      end
      options[:arguments].update(:names_and_values => values)
      # options.update :verbose => true, :verbose_result => true # , :dry_run => true
      conn.run <<"END", options
UPDATE qr_report_runs
SET :SET_VALUES
WHERE id = :qr_run_id
:WHERE?
END
    end

    def select options = nil
      options = _options options
      _select({:order_by => 'ORDER BY qr_run_row'}.merge(options))
    end

    def _select options = nil
      options = _options options
      columns = options[:COLUMNS] || '*'
      columns = conn.safe_sql(columns)
      order_by = conn.safe_sql(options[:order_by] || '')
      options[:arguments].update(
                          :COLUMNS => columns,
                          :ORDER_BY => order_by)
      conn.run "SELECT :COLUMNS FROM #{report_table} WHERE qr_run_id = :qr_run_id :WHERE? :ORDER_BY?", options
    end

    # Deletes this report and its rows.
    def delete! options = nil
      truncate!
      options = _options options
      options.update(:capture_error => true)
      conn.run "DELETE FROM qr_report_runs WHERE id = :qr_run_id", options # .merge(:verbose => true)
      result =
      conn.run "SELECT COUNT(*) AS \"count\" from qr_report_runs WHERE report_table = :report_table",
      :arguments => { :report_table => report_table }, :capture_error => true # , :verbose => true
      if result.rows[0]["count"] <= 0
        conn.run "-- DROP TABLE #{report_table}", :capture_error => true  # , :verbose => true
      end
    end

    # Deletes the actual rows for this report run.
    def truncate! options = nil
      options = _options options
      options.update(:capture_error => true)
      conn.run "DELETE FROM #{report_table} WHERE qr_run_id = :qr_run_id :WHERE?", options
      self
    end
  end
end
