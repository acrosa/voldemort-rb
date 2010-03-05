require File.join(File.dirname(__FILE__), "connection", "voldemort_node")
require File.join(File.dirname(__FILE__), "connection", "connection")
require File.join(File.dirname(__FILE__), "connection", "tcp_connection")

class VoldemortClient
  attr_accessor :connection
  attr_accessor :conflict_resolver

  def initialize(db_name, *hosts, &block)
    self.conflict_resolver = block unless !block
    self.connection = TCPConnection.new(db_name, hosts) # implement and modifiy if you don't want to use TCP protobuf.
    self.connection.bootstrap
  end

  def get(key)
    versions = self.connection.get(key)
    version  = self.resolve_conflicts(versions.versioned)
    if version
      version.value
    else
      nil
    end
  end

  def get_all(keys)
    all_version = self.connection.get_all(keys)
    values = {}
    all_version.values.collect do |v|
      values[v.key] = self.resolve_conflicts(v.versions).value
    end
    values
  end

  def put(key, value, version = nil)
    self.connection.put(key, value)
  end

  def delete(key)
    self.connection.delete(key)
  end

  def resolve_conflicts(versions)
    return self.conflict_resolver.call(versions) if self.conflict_resolver
    # by default just return the version that has the most recent timestamp.
    versions.max { |a, b| a.version.timestamp <=> b.version.timestamp }
  end
end
