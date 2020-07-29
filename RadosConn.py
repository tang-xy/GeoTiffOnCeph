# encoding=utf-8
import rados, sys
from osgeo import gdal
class RadosConn():
    def __init__(self, pool_name):
        cluster = rados.Rados(conffile='/etc/ceph/ceph.conf')
        cluster.connect()
        if not cluster.pool_exists(pool_name):
            raise RuntimeError('No data pool exists')
        self.ioctx = cluster.open_ioctx(pool_name)

    def __del__(self):
        self.ioctx.close()

    def read_image_rados(self, obj_name):
        dic = {}
        dic['data'] = self.ioctx.read(obj_name)
        for k, v in self.ioctx.get_xattrs(obj_name):
            dic[k] = v
        return dic

    def read_all_image_rados(self):
        for obj in self.ioctx.list_objects():
            obj.read()
            dic = {}
            for k, v in obj.get_xattrs():
                dic[k] = v
    
    def read_all_image_key(self):
        keylist = []
        for obj in self.ioctx.list_objects():
            keylist.append(obj.key)
        return keylist
    
    def write_img(self, filepath,im_proj, im_geotrans, b):
        # gdal数据类型包括
        # gdal.GDT_Byte,
        # gdal .GDT_UInt16, gdal.GDT_Int16, gdal.GDT_UInt32, gdal.GDT_Int32,
        # gdal.GDT_Float32, gdal.GDT_Float64

        # 判断栅格数据的数据类型
        if 'int8' in b.dtype.name:
            datatype = gdal.GDT_Byte
        elif 'int16' in b.dtype.name:
            datatype = gdal.GDT_UInt16
        else:
            datatype = gdal.GDT_Float32

        # 判读数组维数
        if len(b.shape) == 3:
            im_bands, im_height, im_width = b.shape
        else:
            im_bands, (im_height, im_width) = 1, b.shape

        # 创建文件
        driver = gdal.GetDriverByName("GTiff")  # 数据类型必须有，因为要计算需要多大内存空间
        dataset = driver.Create(filepath, im_width, im_height, im_bands, datatype)

        dataset.SetGeoTransform(im_geotrans)  # 写入仿射变换参数
        dataset.SetProjection(im_proj)  # 写入投影

        if im_bands == 1:

            dataset.GetRasterBand(1).WriteArray(b)  # 写入数组数据

        else:
            for i in range(im_bands):

                dataset.GetRasterBand(i + 1).WriteArray(b[i])


        del dataset

    def write_image_file(self, path, obj_name):
        dic = {}
        dataset = gdal.Open(path)
        dic['im_width'] = dataset.RasterXSize
        dic['im_height'] = dataset.RasterYSize
        bandda = dataset.GetRasterBand(1)
        #dic['dayData'] = bandda.ReadRaster(0, 0, dic['im_width'], dic['im_height'])#.tobytes().decode('utf-8')
        img = bandda.ReadAsArray(0, 0, dic['im_width'], dic['im_height'])
        dic['im_geotrans'] = dataset.GetGeoTransform()
        dic['im_proj'] = dataset.GetProjection()
        del dataset
        self.ioctx.write(obj_name, img)
        for key,value in dic.items():
            self.ioctx.set_xattr(obj_name, key, str(value))