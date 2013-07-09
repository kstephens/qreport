require 'qreport'

module Qreport
  module Initialization
    def initialize opts = nil
      opts ||= EMPTY_Hash
      initialize_before_opts if respond_to? :initialize_before_opts
      initialize_from_hash! opts
      initialize_after_opts if respond_to? :initialize_after_opts
    end

    def initialize_from_hash! opts
      if opts
        opts.each do | k, v |
          send(:"#{k}=", v)
        end
      end
      self
    end
  end
end
