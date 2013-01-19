module Qreport
  module Model
    attr_accessor :conn
    def conn
      @conn || Qreport::Connection.current
    end
  end
end
