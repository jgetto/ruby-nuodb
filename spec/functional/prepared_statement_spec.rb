require 'spec_helper'
require 'nuodb'

describe NuoDB::PreparedStatement do
  before(:all) do
    @connection = BaseTest.connect
  end

  create_table_all_types_ddl = 'create table TEST_TYPES (f0 SMALLINT, f1 INTEGER, f2 BIGINT, f3 STRING, f4 VARCHAR, f5 LONGVARCHAR, f6 TIME, f7 DATE, f8 TIMESTAMP)'

  after(:all) do
    #@connection.disconnect
  end

  context "creating a prepared statement" do

    before(:each) do
    end

    after(:each) do
    end

    it "should raise an ArgumentError error when provided no sql" do
      lambda {
        @connection.prepare
      }.should raise_error(ArgumentError)
    end

    it "should raise an TypeError error when a string is not provided as an argument" do
      lambda {
        @connection.prepare nil
      }.should raise_error(TypeError)
    end

    #it "should permissibly support a code block after which the prepared statement is automatically finished" do
    #  local_statement = nil
    #  @connection.prepare 'select 1 from dual' do |statement|
    #    statement.count.should eq(-1)
    #    local_statement = statement
    #  end
    #  lambda {
    #    local_statement.count
    #  }.should raise_error(ArgumentError)
    #end

  end

  context "creating tables" do

    before(:each) do
    end

    after(:each) do
      @connection.prepare 'drop table if exists TEST_TYPES' do |statement|
        statement.execute.should be_false
      end

      it "using a prepared statement should return false upon execute" do
        @connection.prepare create_table_all_types_ddl do |statement|
          statement.execute.should be_false
        end
      end

      it "should raise a database error if trying to create a table that already exists" do
        @connection.prepare create_table_all_types_ddl do |statement|
          statement.execute.should be_false
          lambda {
            statement.execute
          }.should raise_error(NuoDB::DatabaseError)
        end

      end

    end
  end

  context "deleting tables" do

    before(:each) do
    end

    after(:each) do
    end

    it "using a prepared statement should return false upon execute" do
      @connection.prepare 'drop table if exists TEST_TYPES' do |statement|
        statement.execute.should be_false
      end
    end

    it "should raise a database exception if the table does not exist and 'if exists' is not used" do
      lambda {
        @connection.prepare 'drop table NO_EXIST' do |statement|
          statement.execute.should be_false
        end
      }.should raise_error(NuoDB::DatabaseError)
    end

  end

  context "inserting into tables" do

    insert_ddl = "create table TEST_TYPES (f1 INTEGER, f2 DOUBLE, f3 STRING, f4 DATE, f5 TIME);"
    drop_table = "drop table if exists TEST_TYPES"
    insert_dml = "insert into test_types(f1, f2, f3, f4, f5) values(?, ?, ?, ?, ?)"
    select_dml = "select * from test_types"

    before(:each) do
      @connection.prepare insert_ddl do |statement|
        statement.execute.should be_false
      end
    end

    after(:each) do
      @connection.prepare drop_table do |statement|
        statement.execute.should be_false
      end
    end

    it "using a prepared statement should return false upon execute" do
      @connection.prepare insert_dml do |statement|
        date = Date.new(2001, 12, 3)
        time = Time.new
        statement.bind_params([56, 6.7, "String", date, time])
        statement.execute.should be_false
        @connection.prepare select_dml do |select|
          select.execute.should be_true
          select.results.each do |row|
            row[0].should eql(56)
            row[1].should eql(6.7)
            row[2].should eql("String")
            row[3].should eql(date)
            row[4].should be_within(1).of(time)
          end
        end
      end
    end

  end

  context "executing a prepared statement" do

    before(:each) do
    end

    after(:each) do
    end

    #it "should return a single row result set for select 1 from dual" do
    #  @connection.prepare 'select 1 from dual' do |statement|
    #    statement.execute.should be_true
    #    results = statement.results
    #    results.finish
    #    results.should_not be_nil
    #  end
    #end

  end

end