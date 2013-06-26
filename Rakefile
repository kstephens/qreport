require "bundler/gem_tasks"
gem 'rspec'
require 'rspec/core/rake_task'

desc "Default => :test"
task :default => :test

desc "Run all tests"
task :test => [ :spec ]

desc "Run specs"
RSpec::Core::RakeTask.new(:spec)

