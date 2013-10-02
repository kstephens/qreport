require 'qreport/report_run'

module Qreport
  class ReportRun
    # Delays pulling in entire result set to determine columns of report result table.
    class Data
      attr_accessor :report_run

      def initialize report_run; @report_run = report_run; end

      def columns
        @columns ||= (@_select || report_run._select(:limit => 0)).columns
      end

      def rows
        @rows ||= _select.rows
      end

      def _select
        @_select ||= report_run._select
      end

      # Delegate all other methods to the Connection::Query object.
      def method_missing sel, *args, &blk
        _select.send(sel, *args, &blk)
      end
    end
  end
end
