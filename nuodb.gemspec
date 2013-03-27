# -*- encoding: utf-8 -*-
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'nuodb/version'

Gem::Specification.new do |spec|
  spec.name        = 'nuodb'
  spec.version     = NuoDB::VERSION
  spec.authors     = ['Robert Buck']
  spec.email       = %w(support@nuodb.com)
  spec.description = %q{An easy to use database API for the NuoDB distributed database.}
  spec.summary     = %q{Native Ruby driver for NuoDB.}
  spec.homepage    = 'http://nuodb.github.com/ruby-nuodb/'
  spec.license     = 'BSD'

  spec.rdoc_options = %w(--charset=UTF-8)
  spec.extra_rdoc_files = %w[AUTHORS README.rdoc LICENSE]

  spec.extensions = %w(ext/nuodb/extconf.rb)

  spec.files = `git ls-files`.split($\)
  spec.test_files = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = %w(lib ext)

  spec.add_development_dependency('erubis', '~> 2.7.0')
  spec.add_development_dependency('rake', '~> 10.0.3')
  spec.add_development_dependency('rdoc', '~> 4.0.0')
  spec.add_development_dependency('hanna-nouveau', '~> 0.2.4')

  %w(rake rdoc simplecov hanna-nouveau rubydoc sdoc).each { |gem| spec.add_development_dependency gem }
  %w(ruby-prof).each { |gem| spec.add_development_dependency gem }
  %w(rspec rspec-core rspec-expectations rspec-mocks).each { |gem| spec.add_development_dependency gem, "~> 2.11.0" }
end
