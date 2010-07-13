require 'rexml/document'
require 'json'

class Connection
  include REXML

  attr_accessor :hosts          # The hosts from where we bootstrapped.
  attr_accessor :nodes          # The array of VoldemortNodes available.
  attr_accessor :db_name        # The DB store name.
  attr_accessor :connected_node # The VoldemortNode we are connected to.
  attr_accessor :request_count  # Used to track the number of request a node receives.
  attr_accessor :request_limit_per_node # Limit the number of request per node.

  STATUS_OK = "ok"
  PROTOCOL  = "pb0"
  DEFAULT_REQUEST_LIMIT_PER_NODE = 500

  def initialize(db_name, hosts, request_limit_per_node = DEFAULT_REQUEST_LIMIT_PER_NODE)
    self.db_name = db_name
    self.hosts   = hosts
    self.nodes   = hosts.collect{ |h|
                    n = h.split(":")
                    node = VoldemortNode.new
                    node.host = n[0]
                    node.port = n[1]
                    node
                   }
    self.request_count = 0
    self.request_limit_per_node = request_limit_per_node
  end

  def bootstrap
    cluster_response = self.get_from("metadata", "cluster.xml", false)
    cluster_xml = cluster_response[1][0][1]
    self.nodes = self.parse_nodes_from(cluster_xml)
    
    stores_response = self.get_from("metadata", "stores.xml", false)
    stores_xml = stores_response[1][0][1]
    
    self.connect_to_random_node
  rescue StandardError => e
    raise("There was an error trying to bootstrap from the specified servers: #{e}")
  end

  def connect_to_random_node
    nodes = self.nodes.sort_by { rand }
    for node in nodes do
      if self.connect_to(node.host, node.port)
        self.connected_node = node
        self.request_count = 0
        return node
      end
    end
  end

  def parse_nodes_from(xml)
    nodes = []
    doc = REXML::Document.new(xml)
    XPath.each(doc, "/cluster/server").each do |n|
      node = VoldemortNode.new
      node.id   = n.elements["id"].text
      node.host = n.elements["host"].text
      node.port = n.elements["socket-port"].text
      node.http_port  = n.elements["http-port"].text
      node.admin_port = n.elements["admin-port"].text
      node.partitions = n.elements["partitions"].text
      nodes << node
    end
    nodes
  end

  def protocol_version
    PROTOCOL
  end

  def connect
    self.connect!
  end

  def reconnect
    self.reconnect!
  end

  def disconnect
    self.disconnect!
  end

  def reconnect_when_errors_in(response = nil)
    return unless response
    self.reconnect! if response.error
  end

  def rebalance_connection?
    self.request_count >= self.request_limit_per_node
  end

  def rebalance_connection_if_needed
    self.reconnect if self.rebalance_connection?
    self.request_count += 1
  end

  def get(key)
    self.rebalance_connection_if_needed
    self.get_from(self.db_name, key, true)
  end

  def get_all(keys)
    self.rebalance_connection_if_needed
    self.get_all_from(self.db_name, keys, true)
  end

  def put(key, value, version = nil, route = true)
    self.rebalance_connection_if_needed
    self.put_from(self.db_name, key, value, version, route)
  end
  
  def delete(key)
    self.delete_from(self.db_name, key)
  end
end
