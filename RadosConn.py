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

    def read_image(self, obj_name):
        dic = {}


    def write_image_file(self, path, obj_name):
        dic = {}
        dataset = gdal.Open(path)
        dic['im_width'] = dataset.RasterXSize
        dic['im_height'] = dataset.RasterYSize
        bandda = dataset.GetRasterBand(1)
        #dic['dayData'] = bandda.ReadRaster(0, 0, dic['im_width'], dic['im_height'])#.tobytes().decode('utf-8')
        img = bandda.ReadRaster(0, 0, dic['im_width'], dic['im_height'])
        #dic['im_geotrans'] = dataset.GetGeoTransform()
        dic['im_proj'] = dataset.GetProjection()
        del dataset

        #self.ioctx.write(obj_name, img)
        #for key,value in dic.items():
            #self.ioctx.set_xattr(obj_name, key, str(value))