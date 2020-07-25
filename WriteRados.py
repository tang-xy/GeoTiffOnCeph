#encoding=utf-8
import rados, sys

cluster = rados.Rados(conffile='/etc/ceph/ceph.conf')
#用当前的这个ceph的配置文件去创建一个rados，这里主要是解析ceph.conf中的   集群配置参数。然后将这些参数的值保存在rados中。
cluster.connect()
#这里将会创建一个radosclient的结构，这里会把这个结构主要包含了几个功能模块：    消息管理模块Messager，数据处理模块Objector，finisher线程模块。

print("\n\nI/O Context and Object Operations")

print("=================================")

print("\nCreating a context for the 'data' pool")

if not cluster.pool_exists('mytest'):

        raise RuntimeError('No data pool exists')

ioctx = cluster.open_ioctx('mytest')
#为一个名字叫做data的存储池创建一个ioctx ，ioctx中会指明radosclient与Objector    模块，同时也会记录data的信息，包括pool的参数等。


print("\nWriting object 'school' with contents 'hello , I are from chd university!' to pool 'data'.")

ioctx.write("school", "hello , I are from chd university!")

ioctx.set_xattr("school", "lang", "en_US")
print("\nWriting object 'name' with contents 'my name is lxl!' to pool 'data'.")

ioctx.write("name", "my name is lxl!")

print("Writing XATTR 'lang' with value 'fr_FR' to object 'name'")

ioctx.set_xattr("name", "lang", "fr_FR")

print("\nContents of object 'school'\n------------------------")

print(ioctx.read("school"))

print(ioctx.get_xattr("name", "lang"))
print("\nContents of object 'name'\n------------------------")

print(ioctx.read("name"))

print("\nClosing the connection.")

ioctx.close()