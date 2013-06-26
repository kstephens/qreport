# -*- encoding: utf-8 -*-
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'qreport/version'

Gem::Specification.new do |gem|
  gem.name          = "qreport"
  gem.version       = Qreport::VERSION
  gem.authors       = ["Kurt Stephens"]
  gem.email         = ["ks.github@kurtstephens.com"]
  gem.description   = %q{Automatically creates materialized report tables from a SQL query.}
  gem.summary       = %q{Automatically creates materialized report tables from a SQL query.}
  gem.homepage      = "http://github.com/kstephens/qreport"

  gem.files         = `git ls-files`.split($/)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.require_paths = ["lib"]

  gem.add_development_dependency 'rake', '>= 0.9.0'
  gem.add_development_dependency 'rspec', '~> 2.12.0'
  gem.add_development_dependency 'simplecov', '~> 0.7.1'

  gem.add_dependency 'pg', '~> 0.14'
end
