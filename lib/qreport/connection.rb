require 'qreport'
require 'time' # iso8601
require 'rational' # Rational
require 'pp' # dump_result!

module Qreport
  class Connection
    attr_accessor :arguments, :env
    attr_accessor :verbose, :verbose_result, :verbose_stream
    attr_accessor :schemaname
    attr_accessor :conn, :conn_owned
    attr_accessor :unescape_value_funcs

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
      @unescape_value_funcs_cache = nil
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
      _close
    ensure
      @invalid = false
    end

    def _close
      if @conn
        conn = @conn
        @conn = nil
        conn.close if @conn_owned
      end
    ensure
      @conn_owned = false
      @transaction_nesting = 0
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
      dump_result! result if @verbose_result || options[:verbose_result]
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

    def escape_value val, example_value = nil
      example_val ||= val
      case val
      when SQL
        val.to_s
      when nil
        NULL
      when true
        T_
      when false
        F_
      when Rational
        val.to_f
      when Numeric
        val
      when String, Symbol
        "'" << conn.escape_string(val.to_s) << QUOTE
      when Time
        escape_value(val.iso8601(6)) << S_TIMESTAMP
      when Range
        "BETWEEN #{escape_value(val.first)} AND #{escape_value(val.last)}"
      when Hash
        escape_value(val.to_json)
      when Array
        case
        when true
          # PUNT!!!
          escape_value(val.to_json)
          # DOES NOT HANDLE EMPTY ARRAY!!!
        when example_val.all?{|x| Numeric === x || x.nil?} && ! example_val.empty?
          "ARRAY[#{val.map{|x| escape_value(x, example_val[0])} * ','}]"
        else
          # PUNT!!!
          escape_value(val.to_json)
        end
      else
        raise TypeError, "cannot escape_value on #{val.class.name}"
      end.to_s
    end
    NULL = 'NULL'.freeze
    QUOTE = "'".freeze
    T_ = "'t'::boolean".freeze
    F_ = "'f'::boolean".freeze
    T = 't'.freeze
    S_TIMESTAMP = "::timestamp".freeze

    def unescape_value val, type
      case val
      when nil
      when String
        return nil if val == NULL
        func = (@unescape_value_funcs_cache ||= { })[type] ||= unescape_value_func(type)
        val = func.call(val, type)
      end
      val
    end

    def unescape_value_func type
      if @unescape_value_funcs and func = @unescape_value_funcs[type]
        return func
      end
      case type
      when /\[\]\Z/
        et = $`
        el = unescape_value_func(et)
        lambda do | val, type |
#          PP.pp([ val, type, et ])
          val.gsub(/\A\{|\}\Z/, '').
            split(',').
            map{|x| x == 'NULL' ? nil : el.call(x, et)}
        end
      when /^bool/
        lambda { | val, type | val == T }
      when /^(int|smallint|bigint|oid|tid|xid|cid)/
        lambda { | val, type | val.to_i }
      when /^(float|real|double|numeric)/
        lambda { | val, type | val.to_f }
      when /^timestamp/
        lambda { | val, type | Time.parse(val) }
      else
        IDENTITY
      end
    end
    IDENTITY = lambda { | val, type | val }

    def verbose_stream
      @verbose_stream || $stderr
    end

    def dump_result! result, stream = nil
      PP.pp(result, stream || verbose_stream) if result
      result
    end

    # Returns a frozen String representing a column type.
    # The String also responds to #pg_ftype and #pg_fmod.
    def type_name *args
      (@type_names ||= { })[args] ||=
        _type_name(args)
    end

    module TypeName
      attr_accessor :pg_ftype, :pg_fmod
    end

    def _type_name args
      x = conn.exec("SELECT pg_catalog.format_type($1,$2)", args).
        getvalue(0, 0).to_s.dup
      # x = ":#{args * ','}" if x.empty? or x == "unknown"
      x.extend(TypeName)
      x.pg_ftype, x.pg_fmod = args
      x.freeze
      x
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
      error = nil
      options ||= EMPTY_Hash
      # $stderr.puts "  run_query! options = #{options.inspect}"
      result = nil
      begin
        result = conn.async_exec(sql)
      rescue ::PG::Error => exc
        error = exc
      rescue ::StandardError => exc
        @invalid = true
        error = exc
      end
      result
    ensure
      if error
        # $stderr.puts "  ERROR: #{exc.inspect}\n  #{exc.backtrace * "\n  "}"
        query.error = error.inspect
        raise error unless options[:capture_error]
      end
    end

  end
end

require 'qreport/connection/query'
