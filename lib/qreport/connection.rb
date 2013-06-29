require 'qreport'
require 'time' # iso8601

module Qreport
  class Connection
    attr_accessor :arguments, :verbose, :verbose_result, :env
    attr_accessor :schemaname
    attr_accessor :conn, :conn_owned

    class << self
      attr_accessor :current
    end

    def initialize args = nil
      @arguments = args
      initialize_copy nil
      if conn = @arguments && @arguments.delete(:conn)
        self.conn = conn
      end
    end

    def initialize_copy src
      @conn = @conn_owned = nil
      @abort_transaction = @invalid = nil
      @transaction_nesting = 0
    end

    def env
      @env || ENV
    end

    def arguments
      @arguments || {
        :host => env['PGHOST'] || 'test',
        :port => env['PGPORT'],
        :user => env['PGUSER'] || 'test',
        :password => env['PGPASSWORD'] || 'test',
        :dbname => env['PGDATABASE'] || 'test',
      }
    end

    # Returns the PG connection object.
    # Create a new connection from #arguments.
    # New connection will be closed by #close.
    def conn
      @conn ||=
        begin
          if @@require_pg
            require 'pg'
            @@require_pg = false
          end
          initialize_copy nil
          c = PG.connect(arguments)
          @conn_owned = true if c
          c
        end
    end
    @@require_pg = true

    # Sets the PG connection object.
    # Connection will not closed by #close.
    def conn= x
      @conn_owned = false
      @conn = x
    end

    def fd
      @conn && @conn.socket
    end

    def close
      raise Error, "close during transaction" if in_transaction?
      if @conn
        conn = @conn
        @conn = nil
        conn.close if @conn_owned
      end
    ensure
      @invalid = false
      @transaction_nesting = 0
      @conn_owned = false
    end

    def in_transaction?; @transaction_nesting > 0; end

    def transaction
      raise Error, "no block" unless block_given?
      abort = false
      begin
        transaction_begin
        yield
      rescue ::StandardError => exc
        abort = @abort_transaction = exc
        raise exc
      ensure
        transaction_end abort
      end
    end

    def transaction_begin
      if @transaction_nesting == 0
        _transaction_begin
      end
      @transaction_nesting += 1
      self
    end

    def transaction_end abort = nil
      if (@transaction_nesting -= 1) == 0
        begin
          if abort
            _transaction_abort
          else
            _transaction_commit
          end
        ensure
          if @invalid
            close
          end
        end
      end
      self
    end

    def _transaction_begin
      run "BEGIN"; self
    end

    def _transaction_commit
      run "COMMIT"; self
    end

    def _transaction_abort
      run "ABORT"; self
    end

    def table_exists? name, schemaname = nil
      schema_name = name.split('.', 2)
      schema = schema_name.shift if schema_name.size > 1
      name = schema_name.first
      schema ||= schemaname || self.schemaname || 'public'
      result =
      run "SELECT EXISTS(SELECT * FROM pg_catalog.pg_tables WHERE tablename = :tablename AND schemaname = :schemaname) as \"exists\"",
      :arguments => { :tablename => name, :schemaname => schema }
      # result.rows; pp result
      result.rows[0]["exists"]
    end

    # options:
    #   :arguments => { :key => value, ... }
    #   :limit => size
    #   :limit => [ size, offset ]
    def run sql, options = nil
      options ||= { }
      conn = options[:connection] || self.conn
      result = Query.new
      result.sql = sql
      result.options = options
      result.conn = self
      result.run!
      dump_result result if @verbose_result || options[:verbose_result]
      result
    end

    # Represents raw SQL.
    class SQL
      def self.new sql; self === sql ? sql : super; end
      def initialize sql; @sql = sql.freeze; end
      def to_s; @sql; end
    end

    def safe_sql x
      SQL.new(x)
    end

    def escape_identifier name
      conn.escape_identifier name.to_s
    end

    def escape_value val
      case val
      when SQL
        val.to_s
      when nil
        NULL
      when true
        T_
      when false
        F_
      when Integer, Float
        val
      when String, Symbol
        "'" << conn.escape_string(val.to_s) << QUOTE
      when Time
        escape_value(val.iso8601(6)) << "::timestamp"
      when Range
        "BETWEEN #{escape_value(val.first)} AND #{escape_value(val.last)}"
      when Hash, Array
        escape_value(val.to_json)
      else
        raise TypeError
      end.to_s
    end
    NULL = 'NULL'.freeze
    QUOTE = "'".freeze
    T_ = "'t'::boolean".freeze
    F_ = "'f'::boolean".freeze
    T = 't'.freeze

    def unescape_value val, type
      case val
      when String
        return nil if val == NULL
        case type
        when "boolean"
          val = val == T
        when /int/
          val = val.to_i
        when "numeric"
          val = val.to_f
        when /timestamp/
          val = Time.parse(val)
        else
          val
        end
      else
        val
      end
    end

    def dump_result result
      pp result if result
      result
    end

    def type_name type, mod
      @type_names ||= { }
      @type_names[[type, mod]] ||=
        conn.exec("SELECT format_type($1,$2)", [type, mod]).
        getvalue(0, 0).to_s.dup.freeze
    end

    def with_limit sql, limit = nil
      sql = sql.dup
      case limit
      when Integer
        limit = "LIMIT #{limit}"
      when Array
        limit = "OFFSET #{limit[1].to_i}\nLIMIT #{limit[0].to_i}"
      end
      unless sql.sub!(/:LIMIT\b|\bLIMIT\s+\S+\s*|\Z/im, "\n#{limit}")
        sql << "\n" << limit
      end
      sql
    end

    def run_query! sql, query, options = nil
      options ||= EMPTY_Hash
      result = nil
      begin
        result = conn.async_exec(sql)
      rescue ::PG::Error => exc
        # $stderr.puts "  ERROR: #{exc.inspect}\n  #{exc.backtrace * "\n  "}"
        query.error = exc.inspect
        raise exc unless options[:capture_error]
      rescue ::StandardError => exc
        @invalid = true
        query.error = exc.inspect
        raise exc unless options[:capture_error]
      end
      result
    end

    class Query
      attr_accessor :conn, :sql, :options
      attr_accessor :sql_prepared
      attr_accessor :error, :cmd_status_raw, :cmd_status, :cmd_tuples
      attr_accessor :nfields, :fields, :ftypes, :fmods
      attr_accessor :type_names
      attr_accessor :columns, :rows

      def run!
        @error = nil
        sql = @sql_prepared = prepare_sql self.sql
        if conn.verbose || options[:verbose]
          $stderr.puts "\n-- =================================================================== --"
          $stderr.puts sql
          $stderr.puts "-- ==== --"
        end
        return self if options[:dry_run]
        if result = conn.run_query!(sql, self, options)
          extract_results! result
        end
        self
      end

      def prepare_sql sql
        sql = sql.sub(/[\s\n]*;[\s\n]*\Z/, '')
        if options.key?(:limit)
          sql = conn.with_limit sql, options[:limit]
        end
        if arguments = options[:arguments]
          if values = arguments[:names_and_values]
            n = conn.safe_sql(values.keys * ', ')
            v = conn.safe_sql(values.keys.map{|k| ":#{k}"} * ', ')
            sql = sql_replace_arguments(sql,
                              :NAMES  => n,
                              :VALUES => v,
                              :NAMES_AND_VALUES => conn.safe_sql("( #{n} ) VALUES ( #{v} )"),
                              :SET_VALUES => conn.safe_sql(values.keys.map{|k| "#{conn.escape_identifier(k)} = :#{k}"} * ', '))
            arguments = arguments.merge(values)
          end
          sql = sql_replace_arguments(sql, arguments)
        end
        sql
      end

      def sql_replace_arguments sql, arguments
        sql = sql.gsub(/(:(\w+)\b([?]?))/) do | m |
          name = $2.to_sym
          optional = ! $3.empty?
          if arguments.key?(name) || optional
            val = arguments[name]
            unless optional && val.nil?
              val = conn.escape_value(val)
            end
            $stderr.puts "  #{name} => #{val}" if options[:verbose_arguments]
            val
          else
            $1
          end
        end
        sql = sql_replace_match sql
      end

      def sql_replace_match sql
        sql = sql.gsub(/:~\s*\{\{([^\}]+?)\}\}\s*\{\{([^\}]+?)\}\}/) do | m |
          expr = $1
          val = $2
          case expr
          when /\A\s*BETWEEN\b/
            "(#{val} #{expr})"
          when "NULL"
            "(#{val} IS NULL)"
          else
            "(#{val} = #{expr})"
          end
        end
        sql
      end

      def extract_results! result
        error = result.error_message
        error &&= ! error.empty? && error
        @error = error
        @cmd_status_raw = result.cmd_status
        @cmd_tuples = result.cmd_tuples
        @nfields = result.nfields
        @ntuples = result.ntuples
        @fields = result.fields
        @ftypes = (0 ... nfields).map{|i| result.ftype(i) }
        @fmods  = (0 ... nfields).map{|i| result.fmod(i) }
        @rows = result.to_a
        result.clear
        self
      end

      def columns
        @columns ||= @fields.zip(type_names)
      end

      def type_names
        @type_names ||= (0 ... nfields).map{|i| conn.type_name(@ftypes[i], @fmods[i])}
      end

      def cmd_status
        @cmd_status ||=
          begin
            x = @cmd_status_raw.split(/\s+/)
            [ x[0] ] + x[1 .. -1].map(&:to_i)
          end.freeze
      end

      def rows
        return @rows if @rows_unescaped
        @rows.each do | r |
          columns.each do | c, t |
            r[c] = conn.unescape_value(r[c], t)
          end
        end
        @rows_unescaped = true
        @rows
      end

    end
  end
end

