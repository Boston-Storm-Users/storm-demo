$LOAD_PATH.unshift File.join(File.dirname(__FILE__), 'gen-rb')

require 'rubygems'
require 'thrift'
require 'distributed_r_p_c'

begin
  socket    = Thrift::Socket.new('localhost', 3772)
  transport = Thrift::FramedTransport.new(socket)
  protocol  = Thrift::BinaryProtocol.new(transport)
  client    = DistributedRPC::Client.new(protocol)

  transport.open

  puts client.execute("dump", "").inspect
  puts client.execute("pathRequests", "").inspect
  puts client.execute("user-agents", "").inspect
  puts client.execute("filtered-user-agents", "Chrome").inspect

  transport.close

rescue Thrift::Exception => tx
  print 'Thrift::Exception: ', tx.message, "\n"
end