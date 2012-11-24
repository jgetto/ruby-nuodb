def with_env_tz(new_tz = 'US/Eastern')
  old_tz, ENV['TZ'] = ENV['TZ'], new_tz
  yield
ensure
  old_tz ? ENV['TZ'] = old_tz : ENV.delete('TZ')
end

@default_timezone = :local

def with_default_timezone(zone)
  old_zone, @default_timezone = @default_timezone, zone
  yield
ensure
  @default_timezone = old_zone
end

def quoted_date(value)
  if value.acts_like?(:time)
    zone_conversion_method = @default_timezone == :utc ? :getutc : :getlocal
    if value.respond_to?(zone_conversion_method)
      value = value.send(zone_conversion_method)
    end
  end
  value.to_s(:db)
end
