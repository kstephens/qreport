# Qreport

Executes a SQL query into a report table.

## Installation

Add this line to your application's Gemfile:

    gem 'qreport'

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install qreport

## Usage

Qreport rewrites a plain SQL query so that its result set can populate a report table.
It automatically creates the report table based on a signature of the column names and types of the query result.
It can also add additional columns to the report table for other uses, for example: batch processing.
Currently supports PostgreSQL.

## Example:

We have users that write articles.
Generate a report named "users_with_articles" of all users that have written an article in N days:

    SELECT
        u.id AS "user_id"
    FROM
        users u
    WHERE 
        EXISTS(SELECT * FROM articles a WHERE a.user_id = u.id AND a.created_on >= NOW() - INTERVAL '30 days')
    ;

Qreport translates this query into:

    SELECT
        0 AS "qr_run_id"
      , nextval('qr_row_seq') AS "qr_row_id"
      , u.id AS "user_id"
    FROM
        users u
    WHERE 
        EXISTS(SELECT * FROM articles a WHERE a.user_id = u.id AND a.created_on >= NOW() - INTERVAL '30 days')
    ;

Then analyzes the columns names and types of this query to produce a unique table name: "users_with_articles_abc123".
The qr_report_runs table keeps track of each report run.
A record is inserted into the qr_report_runs table with an id of 123.
Qreport then executes:

    CREATE TABLE users_with_articles_abc123 AS
    SELECT
        123 AS "qr_run_id"
      , nextval('qr_row_seq') AS "qr_row_id"
      , u.id AS "user_id"
    FROM
        users u
    WHERE 
        EXISTS(SELECT * FROM articles a WHERE a.user_id = u.id AND a.created_on >= NOW() - INTERVAL '30 days')
    ;

Subsequent queries with the same column signature will use "INSERT INTO users_with_articles_abc123".

New queries, rollups and reports can be built from previous reports.

## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request
