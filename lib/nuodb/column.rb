require 'set'

module NuoDB

  class Column
    TRUE_VALUES = [true, 1, '1', 't', 'T', 'true', 'TRUE', 'on', 'ON'].to_set
    FALSE_VALUES = [false, 0, '0', 'f', 'F', 'false', 'FALSE', 'off', 'OFF'].to_set

    module Format
      ISO_DATE = /\A(\d{4})-(\d\d)-(\d\d)\z/
      ISO_DATETIME = /\A(\d{4})-(\d\d)-(\d\d) (\d\d):(\d\d):(\d\d)(\.\d+)?\z/
    end

    attr_reader :name, :default, :type, :limit, :null, :sql_type, :precision, :scale
    attr_accessor :primary, :coder

    alias :encoded? :coder

    # Instantiates a new column in the table.
    #
    # +name+ is the column's name, such as <tt>supplier_id</tt> in <tt>supplier_id int(11)</tt>.
    # +default+ is the type-casted default value, such as +new+ in <tt>sales_stage varchar(20) default 'new'</tt>.
    # +sql_type+ is used to extract the column's length, if necessary. For example +60+ in
    # <tt>company_name varchar(60)</tt>.
    # It will be mapped to one of the standard Rails SQL types in the <tt>type</tt> attribute.
    # +null+ determines if this column allows +NULL+ values.
    def initialize(name, default, sql_type = nil, null = true)
      @name = name
      @sql_type = sql_type
      @null = null
      @limit = extract_limit(sql_type)
      @precision = extract_precision(sql_type)
      @scale = extract_scale(sql_type)
      @type = simplified_type(sql_type)
      @default = extract_default(default)
      @primary = nil
      @coder = nil
    end

    # Returns +true+ if the column is either of type string or text.
    def text?
      type == :string || type == :text
    end

    # Returns +true+ if the column is either of type integer, float or decimal.
    def number?
      type == :integer || type == :float || type == :decimal
    end

    def has_default?
      !default.nil?
    end

    # Returns the Ruby class that corresponds to the abstract data type.
    def klass
      case type
        when :integer then
          Fixnum
        when :float then
          Float
        when :decimal then
          BigDecimal
        when :datetime, :timestamp, :time then
          Time
        when :date then
          Date
        when :text, :string, :binary then
          String
        when :boolean then
          Object
      end
    end

    # Casts value (which is a String) to an appropriate instance.
    def type_cast(value)
      return nil if value.nil?
      return coder.load(value) if encoded?
      value
    end

    def extract_default(default)
      type_cast(default)
    end

    private

    def extract_limit(sql_type)
      $1.to_i if sql_type =~ /\((.*)\)/
    end

    def extract_precision(sql_type)
      $2.to_i if sql_type =~ /^(numeric|decimal|number)\((\d+)(,\d+)?\)/i
    end

    def extract_scale(sql_type)
      case sql_type
        when /^(numeric|decimal|number)\((\d+)\)/i then
          0
        when /^(numeric|decimal|number)\((\d+)(,(\d+))\)/i then
          $4.to_i
      end
    end

    def simplified_type(field_type)
      case field_type
        when /int/i
          :integer
        when /float|double/i
          :float
        when /decimal|numeric|number/i
          extract_scale(field_type) == 0 ? :integer : :decimal
        when /datetime/i
          :datetime
        when /timestamp/i
          :timestamp
        when /time/i
          :time
        when /date/i
          :date
        when /clob/i, /text/i
          :text
        when /blob/i, /binary/i
          :binary
        when /char/i, /string/i
          :string
        when /boolean/i
          :boolean
      end
    end
  end
end
