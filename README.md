## NuoDB - Ruby

{<img src="https://api.travis-ci.org/nuodb/ruby-nuodb.png?branch=master" alt="Build Status" />}[http://travis-ci.org/nuodb/ruby-nuodb]
{<img src="https://gemnasium.com/nuodb/ruby-nuodb.png?travis" alt="Dependency Status" />}[https://gemnasium.com/nuodb/ruby-nuodb]
{<img src="https://codeclimate.com/badge.png" alt="Code Climate" />}[https://codeclimate.com/github/nuodb/ruby-nuodb]

== DESCRIPTION

This is the official Ruby Gem for NuoDB. It is written as a Ruby extension that
wraps the NuoDB C++ API, providing a natural API for Ruby developers.

== BUILDING THE GEM

To compile and test run this command:

    rake clean build rdoc spec

== INSTALLING THE GEM

    NUODB_ROOT=/Users/rbuck/tmp/nuodb gem install nuodb-1.0.1.gem

Or from the source tree:

    NUODB_ROOT=/Users/rbuck/tmp/nuodb gem install pkg/nuodb-1.0.1.gem

== TESTING THE GEM

Start up a minimal chorus as follows:

    java -jar ${NUODB_ROOT}/jar/nuoagent.jar --broker &
    ${NUODB_ROOT}/bin/nuodb --chorus test --password bar --dba-user dba --dba-password baz --verbose debug --archive /var/tmp/nuodb --initialize --force &
    ${NUODB_ROOT}/bin/nuodb --chorus test --password bar --dba-user dba --dba-password baz &

Create a user in the database:

    ${NUODB_ROOT}/bin/nuosql test@localhost --user dba --password baz
    > create user cloud password 'user';
    > exit

Run the tests:

    rake spec

== PUBLISHING THE GEM

=== TAGGING

Tag the product using tags per the SemVer specification; our tags have a v-prefix:

    git tag -a v1.0.1 -m "SemVer Version: v1.0.1"

If you make a mistake, take it back quickly:

    git tag -d v1.0.1
    git push origin :refs/tags/v1.0.1

===PUBLISHING

Here are the commands used to publish:

    gem push pkg/nuodb-1.0.1.gem

== INSPECTING THE GEM

It is often useful to inspect the contents of a Gem before distribution.
To do this you dump the contents of a gem thus:

    gem unpack pkg/nuodb-1.0.1.gem

== INSPECTING THE EXPORTED SYMBOLS

To inspec the symbols on Linux:

    nm -gU ext/nuodb/nuodb.bundle

To inspec the symbols on Mac:

    otool -l ext/nuodb/nuodb.bundle

== REFERENCES
