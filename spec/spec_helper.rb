require 'rubygems'

gem 'simplecov'
require 'simplecov'
SimpleCov.start do
  add_filter "spec/"
end

RSpec.configure do |config|
  config.order = "random"
end

$:.unshift File.expand_path('../../lib', __FILE__)

require 'qreport/report_runner'

