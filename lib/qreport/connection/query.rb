require 'qreport/connection'

module Qreport
  class Connection
    class Query
      attr_accessor :conn, :sql, :options
      attr_accessor :sql_prepared
      attr_accessor :error, :cmd_status_raw, :cmd_status, :cmd_tuples
      attr_accessor :nfields, :fields, :ftypes, :fmods
      attr_accessor :type_names
      attr_accessor :columns, :rows

      def run!
        @error = nil
        @fields = @ftypes = @mods = EMPTY_Array
        @nfields = 0
        sql = @sql_prepared = prepare_sql self.sql
        if conn.verbose || options[:verbose]
          out = conn.verbose_stream
          out.puts "\n-- =================================================================== --"
          out.puts sql
          out.puts "-- ==== --"
        end
        return self if options[:dry_run]
        if result = conn.run_query!(sql, self, options)
          extract_results! result
        end
        self
      ensure
        @conn = nil
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
            conn.verbose_stream.puts "  #{name} => #{val}" if options[:verbose_arguments]
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
        error = nil if error.empty?
        @error = error
        @cmd_status_raw = result.cmd_status
        @cmd_tuples = result.cmd_tuples
        @nfields = result.nfields
        @ntuples = result.ntuples
        @fields = result.fields
        @ftypes = (0 ... nfields).map{|i| result.ftype(i) }
        @fmods  = (0 ... nfields).map{|i| result.fmod(i) }
        @rows = result.to_a
        type_names
        rows
        self
      ensure
        result.clear
        @conn = nil
      end

      def columns
        @columns ||= @fields.zip(type_names)
      end

      def type_names
        @type_names ||= (0 ... nfields).map{|i| @conn.type_name(@ftypes[i], @fmods[i])}
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
        (@rows ||= [ ]).each do | r |
          columns.each do | c, t |
            r[c] = @conn.unescape_value(r[c], t)
          end
        end
        @rows_unescaped = true
        @rows
      end

    end
  end
end

