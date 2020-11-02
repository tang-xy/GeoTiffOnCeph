# coding:utf-8
from SearchEngine import SearchEngine
from Stitching import Stitching
from GridCalculate import GridCalculate
from CoordinateAndProjection import CoordinateAndProjection
from DataStruct import basic_data_struct

from osgeo import gdal

import os

def DealIn10km(sGridCode, sDateTime, iDataProduct, iModelId, iCloulLevel, sDatahomePath, sRslPath):
    '''根据 格网编码 日期 类型， 处理单个10km格网内的业务 ——以基本结构表示的某景影像
    1 根据格网序编号、日期、数据类型得到该网格内待处理数据列表 —Lbds
    2 2 根据待处理数据列表，完成该格网的业务处理                 —bds
    '''
    lbds10kmIn = SearchEngine.SearchByRgDttmDtpd(sGridCode, sDatahomePath, sDateTime, iDataProduct, iCloulLevel)
    if (len(lbds10kmIn) >= 1):
        bdsRlt = Stitching.StichingIn10km(lbds10kmIn,iDataProduct,iModelId,sRslPath)
    else:
        bdsRlt = basic_data_struct()
        bdsRlt.sPathName = "0"
    return bdsRlt

def ProcessInDataBlock(sGridCodeLT, sGridCodeRB, iPCSType, sDateTime, iDataProduct, iModelId, iCloulLevel, sDatahomePath, sRslPath):
    lsGridcode = GridCalculate.GridCodeToGridlist_iPCSType(sGridCodeLT, sGridCodeRB, iPCSType)
    lbdsRlt10kmBtw = []
    for sGridcode in lsGridcode:
        bdsTemp = DealIn10km(sGridcode, sDateTime, iDataProduct, iModelId,iCloulLevel, sDatahomePath, sRslPath)
        if bdsTemp.sPathName != '0':
            lbdsRlt10kmBtw.append(bdsTemp)
    if len(lbdsRlt10kmBtw) != 0:
        return Stitching.StichingBtwn10km(lbdsRlt10kmBtw, iModelId, sRslPath, iPCSType).sPathName
    else:
        return "0"
    
def warp_raster(input_raster_name, input_shape, sRslPath):
    input_raster = gdal.Open(input_raster_name)
    output_raster = os.path.join(sRslPath, os.path.basename(input_raster_name) + "_cil.tif")
    ds = gdal.Warp(output_raster,
              input_raster,
              format = 'GTiff',
              cutlineDSName = input_shape,      # or any other file format
              #cutlineWhere="FIELD = 'whatever'",# optionally you can filter your cutline (shapefile) based on attribute values
              dstNodata = 0)
    del ds

def SubProcessingForGetImg_Unit_Block_FirstLeftZone(pWGSLT, pWGSRB, iPCSType, sDateTime, iDataProduct, iModelId, iCloulLevel, sDatahomePath, sRslPath, input_shape):
    iXBlocksLth = iYBlocksLth = 100
    sGridCodeLT = CoordinateAndProjection.GeoCdnToGridCode(pWGSLT)
    sGridCodeRB = CoordinateAndProjection.GeoCdnToGridCode(pWGSRB)
    lsGridCodeLTRB = GridCalculate.DivideDataBlocks(sGridCodeLT, sGridCodeRB, iXBlocksLth, iYBlocksLth)
    for GridCodeLTRB in lsGridCodeLTRB:
        input_raster_name = ProcessInDataBlock(GridCodeLTRB[0], GridCodeLTRB[1], iPCSType, sDateTime, iDataProduct, iModelId, iCloulLevel, sDatahomePath, sRslPath)
        warp_raster(input_raster_name, input_shape, sRslPath)

def SubProcessingForGetImg_Unit_Block_RightZone(pWGSLT, pWGSRB, pWGSLB, iPCSType, sDateTime, iDataProduct, iModelId, iCloulLevel, sDatahomePath, sRslPath, input_shape):
    iXBlocksLth = iYBlocksLth = 100
    sGridCodeLT = CoordinateAndProjection.GeoCdnToGridCode(pWGSLT)
    sGridCodeRB = CoordinateAndProjection.GeoCdnToGridCode(pWGSRB)
    sGridCodeLB = CoordinateAndProjection.GeoCdnToGridCode(pWGSLB)
    resGridCodeLT =  GridCalculate.LtPointRecaculate(sGridCodeLT, sGridCodeRB, sGridCodeLB)

    lsGridCodeLTRB = GridCalculate.DivideDataBlocks(sGridCodeLT, sGridCodeRB, iXBlocksLth, iYBlocksLth)
    for GridCodeLTRB in lsGridCodeLTRB:
        input_raster_name = ProcessInDataBlock(GridCodeLTRB[0], GridCodeLTRB[1], iPCSType, sDateTime, iDataProduct, iModelId, iCloulLevel, sDatahomePath, sRslPath)
        warp_raster(input_raster_name, input_shape, sRslPath)

def SubProcessingForGetImg_v3_AddModelid(pWGSLT, pWGSRB, iPCSType, sDateTime, iDataProduct, iModelId, iCloulLevel, sDatahomePath, sRslPath, input_shape):
    lsGridcode = GridCalculate.GridCodeToGridlist_iPCSType(pWGSLT, pWGSRB, iPCSType)


def GetImg_Unit_Block(pWGSLT, pWGSRB, sDateTime, iDataProduct, iModelId, iCloulLevel, sDatahomePath, sRslPath, input_shape):
    iaZone = [0,0]
    iaZone[0] = CoordinateAndProjection.LongitudeToUTMProjZone(pWGSLT['dx'])
    iaZone[1] = CoordinateAndProjection.LongitudeToUTMProjZone(pWGSRB['dx'])
    print(iaZone[0])
    print(iaZone[1])
    if (iaZone[0] == iaZone[1]):
        iPCSType = int("32600") + iaZone[0]
        if os.path.exists(sRslPath):
            os.mkdir(os.path.join(sRslPath ,"temp"))
            os.mkdir(os.path.join(sRslPath ,str(iPCSType)))
        SubProcessingForGetImg_Unit_Block_FirstLeftZone(pWGSLT, pWGSRB, iPCSType, sDateTime, iDataProduct, iModelId, iCloulLevel,sDatahomePath, sRslPath, input_shape)
    else:
        iaNumLonBtw = iaZone[1] - iaZone[0]
        if iaNumLonBtw == 1:
            iaPCSType = [32600+i for i in iaZone]
            iLonBtw = iaZone[0] * 6 - 180
            pWGSRB_temp = {}
            pWGSLT_temp = {}
            pWGSLB = {}
            pWGSRB_temp['dx'] = iLonBtw - 0.01
            pWGSRB_temp['dy'] = pWGSRB['dy']
            pWGSLT_temp['dx'] = iLonBtw + 0.01
            pWGSLT_temp['dy'] = pWGSLT['dy']
            pWGSLB['dx'] = pWGSLT_temp['dx']
            pWGSLB['dy'] = pWGSRB['dy']
            if os.path.exists(sRslPath):
                os.mkdir(os.path.join(sRslPath ,"temp"))
                os.mkdir(os.path.join(sRslPath ,str(iaPCSType[1])))
                os.mkdir(os.path.join(sRslPath ,str(iaPCSType[0])))
            SubProcessingForGetImg_Unit_Block_FirstLeftZone(pWGSLT, pWGSRB_temp, iaPCSType[0], sDateTime, iDataProduct, iModelId, iCloulLevel, sDatahomePath, sRslPath, input_shape)
            SubProcessingForGetImg_Unit_Block_RightZone(pWGSLT_temp, pWGSRB, pWGSLB, iaPCSType[1], sDateTime, iDataProduct, iModelId, iCloulLevel, sDatahomePath, sRslPath, input_shape)
        else:
            iaZoneBtw = []
            iaPCSType = [0, 0]
            pWGSRB_temp = {}
            pWGSLT_temp = {}
            iLonBtw = iaZone[0] * 6 - 180
            pWGSRB_temp['dx'] = iLonBtw - 0.01
            pWGSRB_temp['dy'] = pWGSRB['dy']
            iaPCSType[0] = 32600 + iaZone[0]
            if os.path.exists(sRslPath):
                os.mkdir(os.path.join(sRslPath ,"temp"))
                os.mkdir(os.path.join(sRslPath ,str(iaPCSType[0])))
            SubProcessingForGetImg_Unit_Block_FirstLeftZone(pWGSLT, pWGSRB_temp, iaPCSType[0], sDateTime, iDataProduct, iModelId, iCloulLevel, sDatahomePath, sRslPath, input_shape)
            for i in range(iaNumLonBtw):
                iaZoneBtw.append(iaZone[0] + 1 + i)
                iLfLon = iaZoneBtw[i] * 6 - 180 - 6
                if (i != iaNumLonBtw):
                    daiRB = {}
                    daiRB['dx'] = iLfLon + 6 - 0.01
                    daiRB['dy'] = pWGSRB['dy']
                    daiLT = {}
                    daiLT['dx'] = iLfLon + 0.01
                    daiLT['dy'] = pWGSLT['dy']
                    pWGSLB = {}
                    pWGSLB['dx'] = daiLT['dx']
                    pWGSLB['dy'] = daiRB['dy']
                    iaPCSTypeA = 32600 + iaZoneBtw[i]
                    if os.path.exists(sRslPath):
                        os.mkdir(os.path.join(sRslPath ,str(iaPCSType[i])))
                    SubProcessingForGetImg_Unit_Block_RightZone(daiLT, daiRB,pWGSLB, iaPCSTypeA, sDateTime, iDataProduct, iModelId, iCloulLevel, sDatahomePath, sRslPath, input_shape)
            lastLT = {}
            lastLB = {}
            lastLT['dx'] = (iaZone[0] + iaNumLonBtw) * 6 - 180 - 6 + 0.01
            lastLT['dy'] = pWGSLT['dy']
            lastLB['dx'] = (iaZone[0] + iaNumLonBtw) * 6 - 180 - 6 + 0.01
            lastLB['dy'] = pWGSLB['dy']
            iaPCSType[1] = 32600 + (iaZone[0] + iaNumLonBtw)
            if os.path.exists(sRslPath):
                os.mkdir(os.path.join(sRslPath ,str(iaPCSType[1])))
            SubProcessingForGetImg_Unit_Block_RightZone(lastLT, pWGSRB, lastLB, iaPCSType[1], sDateTime, iDataProduct, iModelId, iCloulLevel, sDatahomePath, sRslPath, input_shape)


if __name__ == "__main__": 
    # print(DealIn10km("32644\\4807\\92", "20180627", 1, 0, 6, "H:\\RDCRMG_test_data", "H:\\32652\\2020103100_20180627_Data001_Model000_758_py"))
    pWGSLT = {'dx':83.83, 'dy':44.17}
    pWGSRB = {'dx':84.10, 'dy':44.11}
    GetImg_Unit_Block(pWGSLT, pWGSRB, "20180627", 1, 0, 6,
     "H:\\RDCRMG_test_data", "H:\\32652\\2020103100_20180627_Data001_Model000_758_py",
     r"H:\32652\out.shp")
