import os, sys

dir = os.path.dirname(__file__)
if not dir:
    sys.path.append('./gen-py')
else:
    sys.path.append('%s/%s' % (dir, '/gen-py'))

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from storm import DistributedRPC

socket    = TSocket.TSocket('localhost', 3772)
transport = TTransport.TFramedTransport(socket)
protocol  = TBinaryProtocol.TBinaryProtocol(transport)
client = DistributedRPC.Client(protocol)

try:
    transport.open()

    print client.execute("dump", "")
    print client.execute("pathRequests", "")
    print client.execute("user-agents", "")
    print client.execute("filtered-user-agents", "Chrome")

except Thrift.TException, tx:
    print "%s" % (tx.message)
finally:
    transport.close()
