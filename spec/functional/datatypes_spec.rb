require 'spec_helper'
require 'nuodb'
require 'bigdecimal'

describe NuoDB::Result do

  attr_reader :connection

  before(:all) do
    @connection = BaseTest.connect
  end

  after(:all) do
  end

  context "nuodb naturally handles ruby datatypes" do

    attr_reader :statement
    attr_reader :prepared_statement
    attr_accessor :data_array

    make_type_table_ddl = 'CREATE TABLE test_datatypes (f1 BOOLEAN, f2 DOUBLE, f3 INTEGER, f4 BIGINT, f5 STRING, f6 DATE, f7 TIMESTAMP)' #, f8 NUMERIC)'
    drop_type_table_ddl = "DROP TABLE IF EXISTS test_datatypes"
    insert_dml = "insert into test_datatypes(f1, f2, f3, f4, f5, f6, f7) values(?, ?, ?, ?, ?, ?, ?)" # , f8)  later, ", ?)"
    select_dml = "select * from test_datatypes"
    delete_dml = "delete from test_datatypes"

    machine_bytes = %w(foo).pack('p').size
    machine_bits = machine_bytes * 8
    machine_max_signed = 2**(machine_bits-1) - 1
    machine_max_unsigned = 2**machine_bits - 1
    machine_min_signed = -(2**(machine_bits-1))
    machine_min_unsigned = -(2**machine_bits)
    machine_bits_32 = 32
    machine_max_signed_32 = 2**(machine_bits_32-1) - 1
    machine_max_unsigned_32 = 2**machine_bits_32 - 1
    machine_min_signed_32 = -(2**(machine_bits_32-1))
    machine_min_unsigned_32 = -(2**machine_bits_32)

    before(:all) do
      @data_array = [
          [false, 12345.67890, -1, 222222222, 'Bofur and Bifur', Date.new(1957, 3, 27), Time.new], #, BigDecimal("1000234000567.95")],
          [false, -12345.67890, -1, -222222222, 'Bofur and Bifur', Date.new(1957, 3, 27), Time.new], #, BigDecimal("-1000234000567.95")],
          [false, -0.0, -0, -0, 'Kili and Fili', Date.new(2001, 12, 3), Time.new], #, BigDecimal("-0.0")],
          [true, +0.0, +0, +0, 'Kili and Fili', Date.new(2001, 12, 3), Time.new], #, BigDecimal("+0.0")],
          [false, -1.0, -1, -1, 'Kili and Fili', Date.new(2001, 12, 3), Time.new], #, BigDecimal("-1.0")],
          [true, +1.0, +1, -1, 'Kili and Fili', Date.new(2001, 12, 3), Time.new], #, BigDecimal("+1.0")],
          [false, (1.0 * machine_min_signed_32), machine_min_signed_32, machine_min_signed, 'Kili and Fili', Date.new(2001, 12, 3), Time.new], #, BigDecimal("-1.0")],
          [true, (1.0 * machine_max_signed_32), machine_max_signed_32, machine_max_signed, 'Kili and Fili', Date.new(2001, 12, 3), Time.new], #, BigDecimal("-1.0")],
          [false, (1.0 * machine_min_signed_32), machine_min_signed_32, machine_max_unsigned_32, 'Kili and Fili', Date.new(2001, 12, 3), Time.new], #, BigDecimal("-1.0")],
          [true, (1.0 * machine_max_signed_32), machine_max_signed_32, machine_min_unsigned_32, 'Kili and Fili', Date.new(2001, 12, 3), Time.new], #, BigDecimal("-1.0")],
          [false, (1.0 * machine_min_signed_32), machine_min_signed_32, machine_max_unsigned_32, 'Kili and Fili', Date.new(2001, 12, 3), Time.new], #, BigDecimal("#{machine_max_unsigned}")],
          [true, (1.0 * machine_max_signed_32), machine_max_signed_32, machine_min_unsigned_32, 'Kili and Fili', Date.new(2001, 12, 3), Time.new], #, BigDecimal("#{machine_min_unsigned}")],
      ]
      [-1, 1].each do |sign|
        random = Random.new(Random.new_seed)
        data_array << [rand(2).zero?, sign * random.rand(1.0 * machine_max_signed_32), random.rand(machine_max_signed_32),
                       random.rand(machine_max_signed), 'Kili and Fili', Date.new(2001, 12, 3), Time.new] #, BigDecimal("-1.0")
      end
    end

    before(:each) do
      @statement = @connection.statement
      statement.execute(drop_type_table_ddl)
      statement.execute(make_type_table_ddl)
      @prepared_statement = connection.prepare insert_dml
    end

    after(:each) do
      statement.execute(drop_type_table_ddl)
    end

    # n.b. product defect, set this to false when it is fixed; no numeric support
    jira_db_2288_is_open = true

    it "input data types are be matched with output data types" do
      data_array.each do |binds|
        statement.execute(delete_dml).should be_false
        prepared_statement.bind_params binds
        prepared_statement.execute.should be_false
        statement.execute(select_dml).should be_true
        statement.results.rows.each do |row|
          row[0].should eql(binds[0])
          row[1].should be_within(0.001).of(binds[1])
          row[2].should eql(binds[2])
          row[3].should eql(binds[3])
          row[4].should eql(binds[4])
          row[5].should be_within(1).of(binds[5])
          row[6].should be_within(1).of(binds[6])
          #row[7].should eql(binds[7]) unless jira_db_2288_is_open

          row.each_with_index do |data, idx|
            #unless idx == 7 and jira_db_2288_is_open
            data.should be_a(binds[idx].class)
            #end
          end
        end
      end
    end

    it "string input data types are be matched with type-specific output data types" do
      data_array.each do |binds|
        statement.execute(delete_dml).should be_false
        string_binds = binds.inject([]) do |array, bind|
          array << bind.to_s
        end
        prepared_statement.bind_params string_binds
        prepared_statement.execute.should be_false
        statement.execute(select_dml).should be_true
        statement.results.rows.each do |row|
          row[0].should eql(binds[0])
          row[1].should be_within(0.001).of(binds[1])
          row[2].should eql(binds[2])
          row[3].should eql(binds[3])
          row[4].should eql(binds[4])
          row[5].should be_within(1).of(binds[5])
          row[6].should be_within(1).of(binds[6])
          #row[7].should eql(binds[7]) unless jira_db_2288_is_open

          row.each_with_index do |data, idx|
            #unless idx == 7 and jira_db_2288_is_open
            data.should be_a(binds[idx].class)
            #end
          end
        end
      end
    end

  end

  #context "nuodb naturally handles ruby date/time conversions" do
  #
  #  it "preserves time objects with local_time conversion to default timezone utc" do
  #    with_env_tz 'America/New_York' do
  #      with_default_timezone :utc do
  #        time = Time.local(2000)
  #        #topic = Topic.create('written_on' => time)
  #        #saved_time = Topic.find(topic.id).reload.written_on
  #        #assert_equal time, saved_time
  #        #assert_equal [0, 0, 0, 1, 1, 2000, 6, 1, false, "EST"], time.to_a
  #        #assert_equal [0, 0, 5, 1, 1, 2000, 6, 1, false, "UTC"], saved_time.to_a
  #      end
  #    end
  #  end
  #
  #  it "preserves time objects with time with zone conversion to default timezone utc" do
  #    with_env_tz 'America/New_York' do
  #      with_default_timezone :utc do
  #        Time.use_zone 'Central Time (US & Canada)' do
  #          time = Time.zone.local(2000)
  #          #topic = Topic.create('written_on' => time)
  #          #saved_time = Topic.find(topic.id).reload.written_on
  #          #assert_equal time, saved_time
  #          #assert_equal [0, 0, 0, 1, 1, 2000, 6, 1, false, "CST"], time.to_a
  #          #assert_equal [0, 0, 6, 1, 1, 2000, 6, 1, false, "UTC"], saved_time.to_a
  #        end
  #      end
  #    end
  #  end
  #end
end