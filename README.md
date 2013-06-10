# Ruby/NuoDB Interface [<img src="https://api.travis-ci.org/nuodb/ruby-nuodb.png?branch=master" alt="Build Status" />](http://travis-ci.org/nuodb/ruby-nuodb) [<img src="https://gemnasium.com/nuodb/ruby-nuodb.png?travis" alt="Dependency Status" />](https://gemnasium.com/nuodb/ruby-nuodb) [<img src="https://codeclimate.com/github/nuodb/ruby-nuodb.png" />](https://codeclimate.com/github/nuodb/ruby-nuodb)

This is the official Ruby Gem for [NuoDB](http://www.nuodb.com/). It wraps the
NuoDB C++ API, providing a natural API for Ruby.

To use NuoDB with Rails you will also want the [ActiveRecord NuoDB
Adapter](https://github.com/nuodb/ruby-activerecord-nuodb-adapter)

Note: At this time the Ruby/NuoDB Interface does not support Windows.

## Getting Started

1.  If you haven't already, [Download and Install NuoDB](http://nuodb.com/download-nuodb/)

2.  Install the gem

        gem install nuodb

3.  Use NuoDB in Ruby

        require "nuodb"

        conn = NuoDB::Connection.new(:database => 'test@localhost', :username => 'dba', :password => 'goalie', :schema => 'hockey')
        stmt = conn.statement
        stmt.execute("SELECT * FROM hockey")
        stmt.results.each do |res|
            puts res.inspect
        end


## More Information

*   [Ruby/NuoDB Interface Rdoc](http://nuodb.github.io/ruby-nuodb/rdoc/)
*   [NuoDB Community Forum](http://www.nuodb.com/community/forum.php)
*   [NuoDB Online Documentation](http://www.nuodb.com/community/documentation.php)


## Contributing

See [Contribution](CONTRIBUTION.md) for information about contributing to
the Ruby ActiveRecord NuoDB Adapter.
