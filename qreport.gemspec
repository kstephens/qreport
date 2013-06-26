# -*- encoding: utf-8 -*-
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'qreport/version'

Gem::Specification.new do |gem|
  gem.name          = "qreport"
  gem.version       = Qreport::VERSION
  gem.authors       = ["Kurt Stephens"]
  gem.email         = ["kurt@enova.com"]
  gem.description   = %q{TODO: Write a gem description}
  gem.summary       = %q{TODO: Write a gem summary}
  gem.homepage      = ""

  gem.files         = `git ls-files`.split($/)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.require_paths = ["lib"]

  gem.add_development_dependency 'rake', '>= 0.9.0'
  gem.add_development_dependency 'rspec', '~> 2.12.0'
  gem.add_development_dependency 'simplecov', '~> 0.7.1'

  gem.add_dependency 'pg', '~> 0.14'
end
