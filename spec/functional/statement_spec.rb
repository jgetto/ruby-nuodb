require 'spec_helper'
require 'nuodb'

describe NuoDB::Statement do
  before(:all) do
    @connection = BaseTest.connect
  end

  create_table_all_types_ddl = 'create table TEST_TYPES (f1 INTEGER DEFAULT 4, f2 DOUBLE NOT NULL, f3 STRING, f4 DATE, f5 TIME);'
  check_drop_table_all_types_ddl = 'drop table if exists TEST_TYPES'
  drop_table_all_types_ddl = 'drop table NO_EXIST'
  check_drop_table_all_types_ddl = 'drop table if exists TEST_TYPES'

  after(:all) do
    #@connection.disconnect
  end

  context "creating a statement" do

    before(:each) do
    end

    after(:each) do
    end

    it "should not raise an error when created" do
      lambda {
        @connection.statement
      }.should_not raise_error
    end

    #it "should permissibly support a code block after which the statement is automatically finished" do
    #  local_statement = nil
    #  @connection.statement do |statement|
    #    statement.execute('select 1 from dual').should be_true
    #    local_statement = statement # todo when real ref counting implementation is added
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
      @connection.statement do |statement|
        statement.execute(drop_table_all_types_ddl).should be_false
      end

      it "using a statement should return false upon execute" do
        @connection.statement do |statement|
          statement.execute(create_table_all_types_ddl).should be_false
        end
      end

      it "should raise a database error if trying to create a table that already exists" do
        @connection.statement do |statement|
          statement.execute(create_table_all_types_ddl).should be_false
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

    it "using a statement should return false upon execute" do
      @connection.statement do |statement|
        statement.execute(check_drop_table_all_types_ddl).should be_false
      end
    end

    it "should raise a database exception if the table does not exist and 'if exists' is not used" do
      lambda {
        @connection.statement do |statement|
          statement.execute(drop_table_all_types_ddl).should be_false
        end
      }.should raise_error(NuoDB::DatabaseError)
    end

  end

  context "inserting into tables" do

    insert_dml = "insert into test_types(f1, f2, f3, f4, f5) values(?, ?, ?, ?, ?)"
    select_dml = "select * from test_types"

    before(:each) do
      @connection.statement do |statement|
        statement.execute(create_table_all_types_ddl).should be_false
      end
    end

    after(:each) do
      #@connection.statement do |statement|
      #  statement.execute(check_drop_table_all_types_ddl).should be_false
      #end
    end

    it "using a statement should return false upon execute" do
      @connection.prepare insert_dml do |statement|
        date = Date.new(2001, 12, 3)
        time = Time.new
        statement.bind_params([56, 6.7, "String", date, time])
        statement.execute.should be_false
        @connection.statement do |select|
          select.execute(select_dml).should be_true
          results = select.results
          results.each do |row|
            row[0].should eql(56)
            row[1].should eql(6.7)
            row[2].should eql("String")
            row[3].should eql(date)
            row[4].should be_within(1).of(time)
          end
          columns = results.columns
          results.columns.should eql(columns)

          columns[0].name.should eql("F1")
          columns[1].name.should eql("F2")
          columns[2].name.should eql("F3")
          columns[3].name.should eql("F4")
          columns[4].name.should eql("F5")

          columns[0].type.should eql(:integer)
          columns[1].type.should eql(:float)
          columns[2].type.should eql(:string)
          columns[3].type.should eql(:date)
          columns[4].type.should eql(:time)

          columns[0].default.should eql(4)
          columns[1].default.should be_nil
          columns[2].default.should be_nil
          columns[3].default.should be_nil
          columns[4].default.should be_nil

          columns[0].null.should be_true
          columns[1].null.should be_false
          columns[2].null.should be_true
          columns[3].null.should be_true
          columns[4].null.should be_true

          columns.each do |column|
            column.name.should_not be_nil
          end
        end
      end
    end

  end

  # TODO BEGIN

  # Unsure if this should pass or not; an outstanding question was sent to MJ and John
  # about the intended behavior.

  #context "executing a prepared statement" do
  #
  #  insert_dml = "insert into test_types(f1, f2, f3, f4, f5) values(?, ?, ?, ?, ?)"
  #
  #  before(:each) do
  #    @connection.statement do |statement|
  #      statement.execute(create_table_all_types_ddl).should be_false
  #    end
  #  end
  #
  #  after(:each) do
  #    #@connection.statement do |statement|
  #    #  statement.execute(check_drop_table_all_types_ddl).should be_false
  #    #end
  #  end
  #
  #  it "should return a single row result set for select 1 from dual" do
  #    @connection.statement do |statement|
  #      statement.execute(insert_dml).should be_true
  #      results = statement.results
  #      #results.should_not be_nil
  #    end
  #  end
  #
  #end

  # TODO END

end