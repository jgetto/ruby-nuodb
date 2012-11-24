require 'yaml'
require 'erubis'
require 'fileutils'
require 'pathname'

SPEC_ROOT = File.expand_path(File.dirname(__FILE__))

class Hash
  def symbolize_keys!
    keys.each do |key|
      self[(key.to_sym rescue key) || key] = delete(key)
    end
    self
  end
end

module BaseTest
  class << self
    def config
      @config ||= read_config
    end

    private

    def config_file
      Pathname.new(ENV['NUODB_CONFIG'] || File.join(SPEC_ROOT, 'config.yml'))
    end

    def read_config
      unless config_file.exist?
        FileUtils.cp(File.join(SPEC_ROOT, 'config.example.yml'), config_file)
      end

      erb = Erubis::Eruby.new(config_file.read)
      expand_config(YAML.parse(erb.result(binding)).transform)
    end

    def expand_config(config)
      config
    end
  end
end
