# coding:utf-8
from osgeo import gdal

class BaseProcesses:
    
    @staticmethod
    def CreateNewImage(sImageFormat, sFilePathNew, dGeoTransform, sGeoProjectionRef, iColumnRange, iRowRange, iBandNum, dtty):
        driver = gdal.GetDriverByName(sImageFormat)
        dataset = driver.Create(sFilePathNew, int(iColumnRange), int(iRowRange), int(iBandNum), dtty)
        dataset.SetGeoTransform(dGeoTransform)
        dataset.SetProjection(sGeoProjectionRef)
        return dataset