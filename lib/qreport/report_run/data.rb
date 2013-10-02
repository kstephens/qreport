require 'qreport/report_run'

module Qreport
  class ReportRun
    # Delays pulling in entire result set to determine columns of report result table.
    class Data
      attr_accessor :report_run

      def initialize report_run; @report_run = report_run; end

      def columns; _select0.columns; end
      def type_names; _select0.type_names; end

      def rows
        @rows ||= _select.rows
      end

      # Delegate all other methods to the Connection::Query object.
      def method_missing sel, *args, &blk
        _select.send(sel, *args, &blk)
      end

private

      def _select
        @_select ||= report_run._select
      end

      def _select0
        @_select0 ||= (@_select || report_run._select(:limit => 0))
      end

    end
  end
end
