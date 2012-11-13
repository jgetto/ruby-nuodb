# -*- encoding: utf-8 -*-
require File.expand_path('../lib/nuodb/version', __FILE__)

Gem::Specification.new do |gem|
  gem.name        = 'nuodb'
  gem.version     = NuoDB::VERSION
  gem.authors     = %w('Robert Buck')
  gem.email       = %w(support@nuodb.com)
  gem.description = %q{An easy to use database API for the NuoDB distributed database.}
  gem.summary     = %q{Native Ruby driver for NuoDB.}
  gem.homepage    = 'http://nuodb.github.com/ruby-nuodb/'
  gem.license     = 'BSD'

  gem.rdoc_options = %w(--charset=UTF-8)
  gem.extra_rdoc_files = %w[README.rdoc]

  gem.extensions = %w(ext/nuodb/extconf.rb)

  gem.add_development_dependency('rake', '~> 0.9')
  gem.add_development_dependency('rdoc', '~> 3.10')

  gem.files = `git ls-files`.split($\)
  gem.test_files = gem.files.grep(%r{^(test|spec|features)/})
  gem.require_paths = %w(lib ext)
end
