require 'spec_helper'
require 'qreport/connection'

describe Qreport::Connection do
  QREPORT_TEST_CONN = [ nil ]
  def conn
    QREPORT_TEST_CONN[0] ||= Qreport::Connection.new
  end

  it "can to connect to a test database." do
    conn.conn.class.should == PG::Connection
  end

  it "can manage transaction state." do
    conn.should_receive(:_transaction_begin).once
    conn.should_receive(:_transaction_commit).once
    conn.should_receive(:_transaction_abort).exactly(0).times
    conn.in_transaction?.should == false
    conn.transaction do
      conn.in_transaction?.should == true
    end
    conn.in_transaction?.should == false
  end

  it "can manage nested transactions." do
    conn.should_receive(:_transaction_begin).once
    conn.should_receive(:_transaction_commit).once
    conn.should_receive(:_transaction_abort).exactly(0).times
    conn.in_transaction?.should == false
    conn.transaction do
      conn.in_transaction?.should == true
      conn.transaction do
        conn.in_transaction?.should == true
      end
      conn.in_transaction?.should == true
    end
    conn.in_transaction?.should == false
  end

  it "can manage transaction state during raised exceptions" do
    conn.should_receive(:_transaction_begin).once
    conn.should_receive(:_transaction_commit).exactly(0).times
    conn.should_receive(:_transaction_abort).once
    lambda do
      conn.transaction do
        raise Qreport::Error, "#{__LINE__}"
      end
    end.should raise_error(Qreport::Error)
    conn.in_transaction?.should == false
  end

  it "can dup to create another connection." do
    conn1 = Qreport::Connection.new
    conn1.fd.should == nil
    conn1.conn
    conn1.fd.class.should == Fixnum
    conn2 = nil
    conn1.transaction do
      conn1.in_transaction?.should == true
      conn2 = conn1.dup
      conn2.in_transaction?.should == false
    end
    conn1.in_transaction?.should == false
    conn2.fd.should == nil
    conn2.conn
    conn2.fd.class.should == Fixnum
    conn2.fd.should_not == conn1.fd
    conn2.in_transaction?.should == false
  end

  it 'can set conn.' do
    conn1 = conn
    conn1.conn.class.should == PG::Connection
    conn2 = Qreport::Connection.new(:conn => conn1.conn)
    conn2.conn.object_id.should == conn1.conn.object_id
    conn2.conn_owned.should be_false
  end

  it 'can close conn.' do
    conn.conn.class.should == PG::Connection
    conn.conn_owned.should_not be_false
    conn.close
    conn.instance_variable_get('@conn').should == nil
    conn.conn_owned.should be_false
    conn.close
    conn.instance_variable_get('@conn').should == nil
  end

  describe "#escape_value/#unescape_value" do
  [
    [ nil, 'NULL' ],
    [ true, "'t'::boolean" ],
    [ false, "'f'::boolean" ],
    [ 1234, '1234' ],
    [ -1234, '-1234' ],
    [ 1234.45, '1234.45' ],
    [ :IGNORE, '1234.56::float', 1234.56 ],
    [ :IGNORE, '1234.56::float4', 1234.56 ],
    [ :IGNORE, '1234.56::float8', 1234.56 ],
    [ "string with \", \\, and \'", "'string with \", \\, and '''" ],
    [ :a_symbol!, "'a_symbol!'", :a_symbol!.to_s ],
    [ Time.parse('2011-04-27T13:23:00.000000Z'), "'2011-04-27T13:23:00.000000Z'::timestamp", Time.parse('2011-04-27T13:23:00.000000') ],
    [ Time.parse('2011-04-27 13:23:00 -0500'), "'2011-04-27T13:23:00.000000-05:00'::timestamp", Time.parse('2011-04-27 13:23:00 -0500') ],
    [ [ 1, "2", :three ], "'[1,\"2\",\"three\"]'", :IGNORE ],
    [ { :a => 1, "b" => 2 }, "'{\"a\":1,\"b\":2}'", :IGNORE ],
  ].each do | value, sql, return_value, sql_expr, sql_value |
    it "can handle encoding #{value.class.name} value #{value.inspect} as #{sql.inspect}." do
      pending "not supported", :if => value == :IGNORE
      conn.escape_value(value).should == sql
    end

    sql_expr = sql
    sql_value = return_value
    sql_value = nil if sql_value == :IGNORE
    sql_value ||= value
    it "can handle decoding #{sql_expr.inspect} as #{sql_value.inspect}." do
      pending "not supported", :if => return_value == :IGNORE
      sql_x = conn.escape_value(sql_expr)
      r = conn.run "SELECT #{sql_x} AS value"
      PP.pp r.columns
      r = r.rows.first.values.first
      r.should == sql_value
    end
  end
    it "raises TypeError for other values." do
      lambda do
        conn.escape_value(Object.new)
      end.should raise_error(TypeError)
    end
  end

end
