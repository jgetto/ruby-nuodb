require 'spec_helper'
require 'nuodb'

describe NuoDB::Connection do
  before do
  end

  after do
  end

  context "creating a connection" do

    before(:each) do
    end

    after(:each) do
    end

    it "should raise an ArgumentError error when provided no configuration" do
      lambda {
        NuoDB::Connection.new
      }.should raise_error(ArgumentError)
    end

    it "should raise an DatabaseError error when provided invalid credentials" do
      lambda {
        config = BaseTest.connection_config.clone
        config[:password] = 'invalid'
        NuoDB::Connection.new config do |connection|
          connection.connected?.should be_true
        end
      }.should raise_error(NuoDB::DatabaseError)
    end

    #it "should permissibly support a code block after which the connection is automatically disconnected" do
    #  local_connection = nil
    #  NuoDB::Connection.new BaseTest.connection_config do |connection|
    #    connection.connected?.should be_true
    #    local_connection = connection # todo when real ref counting implementation is added
    #  end
    #  local_connection.connected?.should be_false
    #end

    # decision: we will not support this as we cannot guarantee that there are
    # no outstanding references for the object, say for instance, the user
    # passes a variable out of scope, and as the outer variable does not create
    # a copy of the VALUE object (it is identical from what I see) there is no
    # way to detect closure.
    #
    #it "should permissibly support a code block after which the connection is automatically disconnected even if an exception is raised" do
    #  @local_connection = nil
    #  lambda {
    #    NuoDB::Connection.new BaseTest.connection_config do |connection|
    #      connection.connected?.should be_true
    #      @local_connection = connection # todo when real ref counting implementation is added
    #      raise ArgumentError
    #    end
    #  }.should raise_error(ArgumentError)
    #  @local_connection.connected?.should be_false
    #end

  end

  context "active connections" do

    before(:each) do
      @connection = BaseTest.connect
    end

    after(:each) do
      #@connection.disconnect
      @connection = nil
    end

    it "should yield true when connected? is called" do
      @connection.connected?.should be_true
    end

    it "should yield true when ping is called" do
      @connection.ping.should be_true
    end

    it "support configuring autocommit at the connection level" do
      @connection.autocommit = true
      @connection.autocommit?.should be_true

      @connection.autocommit = false
      @connection.autocommit?.should be_false
    end

  end

  #context "inactive connections" do
  #
  #  before(:each) do
  #    @connection = BaseTest.connect
  #    @connection.disconnect # todo when real ref counting implementation is added
  #  end
  #
  #  after(:each) do
  #    @connection = nil
  #  end
  #
  #  it "should yield false when connected? is called" do
  #    @connection.connected?.should be_false
  #  end
  #
  #end

end