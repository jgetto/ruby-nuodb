module NuoDB

  class DatabaseError < StandardError

    attr_accessor :error_code

    def initialize(message=nil, error_code = nil)
      super(message)
      @error_code = error_code
    end

  end

end