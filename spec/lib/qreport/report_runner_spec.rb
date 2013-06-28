require 'spec_helper'

describe Qreport::ReportRunner do
  attr :reports, :now

  it "should generate two reports" do
    # conn.verbose = conn.verbose_result = true
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

    reports.values.each do | r |
      r.delete!
    end
  end

  it "should DROP TABLE after all report runs are deleted." do
    run_reports!
    reports.values.each do | r |
      r.delete!
    end
    conn.run("SELECT COUNT(*) AS c FROM qr_report_runs").rows[0]["c"].should == 0
  end

  it "should capture errors into ReportRun#error." do
    # conn.verbose = true
    report_run = Qreport::ReportRun.new(:name => :users_with_articles, :description => '10 days')
    report_run.arguments = {
      :now => conn.safe_sql("unknown_column"),
      :interval => '10 days',
    }
    report_run.sql = <<"END"
    SELECT u.id AS "user_id"
    FROM   users u
    WHERE
      EXISTS(SELECT * FROM articles a WHERE a.user_id = u.id AND a.created_on >= :now - INTERVAL :interval)
END
    report_run.run! conn
    report_run.error.class.should == Hash
    report_run.error[:error_class].should == 'PG::Error'
    report_run.error[:error_message].should =~ /column "unknown_column" does not exist/

    report_run.delete!
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
      report_run = Qreport::ReportRun.new(:name => :users_with_articles, :description => interval)
      report_run.arguments = {
        :now => now,
        :interval => interval,
      }
      report_run.sql = sql
      report_run.additional_columns = [
        [ 'qr_processing_status_id', "integer",                  1 ],
        [ 'qr_processed_at',         "timestamp with time zone", nil ],
        [ 'qr_processing_error',     "text", nil ],
      ]
      report_run.run! conn

      # puts "\n  ReportRun #{report_run.id}"
      # pp report_run
      # pp report_run.select(:limit => [10, 2]).rows

      reports[interval] = report_run
    end
  end

  attr :conn, :now

  before :all do
    @conn = Qreport::Connection.new
    # conn.verbose = true

    if conn.table_exists? "qr_report_runs"
      conn.run "DROP TABLE qr_report_runs"
      conn.run "DROP SEQUENCE qr_report_runs_pkey"
    end

    conn.transaction do
      Qreport::ReportRun.schema! conn
    end

    if conn.table_exists? "users"
      conn.run "DROP TABLE users"
      conn.run "DROP SEQUENCE users_pkey"
      conn.run "DROP TABLE articles"
      conn.run "DROP SEQUENCE articles_pkey"
    end

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

  end

  before :each do
    @conn = Qreport::Connection.new
    @now = Time.now

    conn.run "DELETE FROM qr_report_runs"
    conn.run "DELETE FROM articles"
    conn.run "DELETE FROM users"

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
end