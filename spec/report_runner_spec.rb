require 'spec_helper'

describe Qreport::ReportRunner do
  attr :reports, :now

  it "should generate two reports" do
    run_reports!
    reports.size.should == 4

    r = reports['1 days']
    r.select.rows.map{|x| x["user_id"]}.should == [ 2 ]

    r = reports['2 days']
    r.select.rows.map{|x| x["user_id"]}.should == [ 2, 3 ]

    r = reports['30 days']
    r.select.rows.map{|x| x["user_id"]}.should == (1..10).to_a
    r.select(:limit => 2).rows.map{|x| x["user_id"]}.should == [ 1, 2 ]
    r.select(:limit => [ 2, 2 ]).rows.map{|x| x["user_id"]}.should == [ 3, 4 ]
    r.select(:limit => [ 4, 2 ]).rows.map{|x| x["user_id"]}.should == [ 3, 4, 5, 6 ]

    r = reports['60 days']
    r.select.rows.map{|x| x["user_id"]}.should == (1..10).to_a
  end

  it "should DROP TABLE after all report runs are deleted." do
    run_reports!
    reports.values.each do | r |
      r.delete!
    end
    conn.run("SELECT COUNT(*) AS c FROM qr_report_runs").rows[0]["c"].should == 0
  end

  def run_reports!
    @reports = { }

    sql = <<"END"
    SELECT u.id AS "user_id"
    FROM   users u
    WHERE
      EXISTS(SELECT * FROM articles a WHERE a.user_id = u.id AND a.created_on >= :now - INTERVAL :interval)
    ;
END

    [ '1 days', '2 days', '30 days', '60 days' ].each do | interval |
      report_run = Qreport::ReportRun.new
      report_run.name = :new_customer_loan_counts
      report_run.description = interval
      report_run.arguments = {
        :now => now,
        :interval => interval,
      }
      report_run.sql = sql

      runner = Qreport::ReportRunner.new
      report_run.additional_columns = [
        [ 'qr_processing_status_id', "integer",                  1 ],
        [ 'qr_processed_at',         "timestamp with time zone", nil ],
        [ 'qr_processing_error',     "text", nil ],
      ]
      runner.connection = conn
      begin
        runner.run!(report_run)
      rescue ::Exception => exc
        $stderr.puts "  ERROR: #{exc.inspect}"
        raise exc
      end

      # puts "\n  ReportRun #{report_run.id}"
      # pp report_run
      # pp report_run.select(:limit => [10, 2]).rows

      reports[interval] = report_run
    end
  end

  attr :conn, :now
  before :each do
    @now = Time.now
    @conn = Qreport::Connection.new
    # conn.verbose = true

    begin
      Qreport::ReportRun.schema! conn
    rescue ::PG::Error
    end

    conn.transaction_begin

    conn.run <<"END"
CREATE SEQUENCE users_pkey;
CREATE TABLE users (
    id           INTEGER PRIMARY KEY DEFAULT nextval('users_pkey')
  , name         VARCHAR(255) NOT NULL
);
CREATE SEQUENCE articles_pkey;
CREATE TABLE articles (
    id           INTEGER PRIMARY KEY DEFAULT nextval('articles_pkey')
  , user_id      INTEGER
  , name         VARCHAR(255) NOT NULL
  , body         TEXT
  , created_on   TIMESTAMP WITH TIME ZONE NOT NULL
);
END
    (1 .. 10).each do | i |
      conn.run "INSERT INTO users :NAMES_AND_VALUES",
      :arguments => { :names_and_values => {
          :id => i,
          :name => "user#{i}",
        } }
    end
    (1 .. 100).each do | i |
      user_id = (i % 10) + 1
      conn.run "INSERT INTO articles :NAMES_AND_VALUES",
      :arguments => { :names_and_values => {
          :id => i,
          :user_id => user_id,
          :name => "Article #{i}",
          :created_on => now - i * 86000,
        } }
    end
  end

  after :each do
    conn.transaction_end :abort
  end
end
