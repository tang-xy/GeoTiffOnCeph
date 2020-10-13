from thrift.transport import TSocket,TTransport
from thrift.protocol import TBinaryProtocol
from hbase.ttypes import ColumnDescriptor
from hbase import Hbase

def createTable(client, column_name, table_name):
    column = ColumnDescriptor(name = column_name)

    client.createTable(table_name, [column])

if __name__ == "__main__":
    transport = TSocket.TSocket('localhost', 9090)

    transport = TTransport.TBufferedTransport(transport)

    protocol = TBinaryProtocol.TBinaryProtocol(transport)

    client = Hbase.Client(protocol)
    transport.open()


    contents = ColumnDescriptor(name='gf1', maxVersions=1)
    client.createTable('image', [contents])

    print(client.getTableNames())

    transport.close()