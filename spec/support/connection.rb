require 'logger'

module BaseTest
  def self.connection_name
    ENV['NUODB_CONN'] || config['default_connection']
  end

  def self.connection_config
    config['connections'][connection_name].symbolize_keys!
  end

  def self.connect
    NuoDB::Connection.new connection_config
  end
end
