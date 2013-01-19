require 'spec_helper'
require 'qreport/connection'

describe Qreport::Connection do
  attr :conn
  it "can to connect to a test database." do
    @conn = Qreport::Connection.new
    @conn.conn.class.should == PG::Connection
  end

  it "can manage transaction state." do
    @conn = Qreport::Connection.new
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
    @conn = Qreport::Connection.new
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
    @conn = Qreport::Connection.new
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

end
