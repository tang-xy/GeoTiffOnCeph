# coding:utf-8
from DataStruct import basic_data_struct, DataStruct
from GridCalculate import GridCalculate
from BaseProcesses import BaseProcesses
from CoordinateAndProjection import CoordinateAndProjection
import conf

import numpy as np
from osgeo import gdal

import os
import random
import copy
import time
class Stitching():

    @staticmethod
    def get_lefttop_rightbttn_Grid(lbdsBtw10km):
        '''获取序列中最左上角格网和右下角格网
           输入 lbdsBtw10km：basic_data_struct list
           输出 左上角格网，右下角格网
        '''
        bdsLeftTop = basic_data_struct()
        bdsRightBttn = basic_data_struct()
        sGridCode = lbdsBtw10km[0].sGridCode
        min_x = max_x = sGridCode[2: 4] + sGridCode[5: 6]
        min_y = max_y = sGridCode[0: 2] + sGridCode[4: 5]
        for bdsBtw10km in lbdsBtw10km:
            sGridCode = bdsBtw10km.sGridCode
            min_x = min(min_x, sGridCode[2: 4] + sGridCode[5: 6])
            min_y = min(min_y, sGridCode[0: 2] + sGridCode[4: 5])
            max_x = max(max_x, sGridCode[2: 4] + sGridCode[5: 6])
            max_y = max(max_y, sGridCode[0: 2] + sGridCode[4: 5])

        bdsLeftTop.sGridCode = max_y[0: 2] + min_x[0: 2] + max_y[-1] + min_x[-1]
        bdsRightBttn.sGridCode = min_y[0: 2] + max_x[0: 2] + min_y[-1] + max_x[-1]

        return bdsLeftTop, bdsRightBttn


    @staticmethod
    def StichingBtwn10km(lbds, iModelId, sRslPath, iPCSType):
        '''接收符合拼接条件10公里格网序列、拼接结果的存储路径、所处理格网的UTM带号，组织10km间的拼接逻辑
           1 确定最左上角格网                                       --bdsLT(sGridCode)
           2 确定最右下角格网                                       --bdsRB(sGridCode)
           3 根据最左上角格网，求算各待镶嵌影像的RB,CB,             --lbdsBtw10km(RB,CB)
           4 根据最左上最右下格网，求算结果影像的RB,CB,RR,CR        --bdsRlt(all,except patnname)
           5 确定新影像的文件名(只剩下pathname,其他在4中已经改了)   --bdsRlt(pathName)
           6 生成空的新影像                                         --datasetRlt
           7 循环lbds,将值复制到新影像                              --bdsRlt
        '''
        bdsRlt = basic_data_struct()
        bdsLeftTop, bdsRightBttn = Stitching.get_lefttop_rightbttn_Grid(lbds)
        lbds = Stitching.CalculateRBandCBforLbds10km(lbds, bdsLeftTop, lbds[0].iResolution)
        bdsRlt = Stitching.CalculateBegAndRangeForbdsRlt(lbds[0], bdsLeftTop, bdsRightBttn, lbds[0].iResolution)
        bdsRlt = Stitching.GenerateResultName(bdsRlt, lbds[0], sRslPath, iPCSType)
        iRltDataType = Stitching.JudgeResultDataType(lbds[0].iDataProduct, iModelId)
        iBandNum = Stitching.CreateResultImg_2(bdsRlt, lbds[0], iRltDataType)
        Stitching.ModelOfIntegrate_3(bdsRlt, lbds, iRltDataType, iBandNum)

        return bdsRlt

    @staticmethod
    def ModelOfIntegrate_3(bdsRlt, lbdsBeDeal, iRltDataType, iBandNum):
        '''影像集成模型,以波段为循环基准，进行文件开关优化,并进行结果影像类型判断'''
        dtsImg = [gdal.Open(bdsBeDeal.sPathName) for bdsBeDeal in lbdsBeDeal]
        dsRlt = gdal.Open(bdsRlt.sPathName, gdal.GA_Update)
        
        for b in range(1, iBandNum + 1):
            if (iRltDataType == 1 or iRltDataType == 2):
                uiDatabuffer_rlt = np.zeros(bdsRlt.iColumnRange * bdsRlt.iRowRange, dtype=np.int16)
            elif (iRltDataType == 3 or iRltDataType == 4):
                uiDatabuffer_rlt = np.zeros(bdsRlt.iColumnRange * bdsRlt.iRowRange, dtype=np.float64)
            for i in range(len(lbdsBeDeal)):
                bdsTemp = lbdsBeDeal[i]
                uiDatabuffer_temp = dtsImg[i].GetRasterBand(b).ReadAsArray(0, 0, bdsTemp.iColumnRange, bdsTemp.iRowRange)
                for n in range(bdsTemp.iRowRange):
                    for m in range(bdsTemp.iColumnRange):
                        v = int(bdsRlt.iColumnRange * (n + (bdsTemp.iRowBeg - bdsRlt.iRowBeg)) + (m + (bdsTemp.iColumnBeg - bdsRlt.iColumnBeg)))
                        if (uiDatabuffer_temp[m][n] != 0):
                            uiDatabuffer_rlt[v] = uiDatabuffer_temp[m][n]
            # dsRlt.GetRasterBand(b).WriteRaster(0, 0, bdsRlt.iColumnRange, bdsRlt.iRowRange, uiDatabuffer_rlt, bdsRlt.iColumnRange, bdsRlt.iRowRange)            
            uiDatabuffer_rlt_reshape = uiDatabuffer_rlt.reshape((bdsRlt.iColumnRange, bdsRlt.iRowRange))
            dsRlt.GetRasterBand(b).WriteArray(uiDatabuffer_rlt_reshape, 0, 0)            
        del dsRlt
        for img in dtsImg:
            del img

    @staticmethod
    def CreateResultImg_2(bdsRlt, bdsReferance, iRltDataType):
        dtsImg_1 =  gdal.Open(bdsReferance.sPathName)
        dGeoTransform = list(dtsImg_1.GetGeoTransform())
        iBandNumRef = dtsImg_1.RasterCount
        sGeoProjectionRef = dtsImg_1.GetProjectionRef()
        dGeoTransform[0] = dGeoTransform[0] + (bdsRlt.iColumnBeg - bdsReferance.iColumnBeg) *\
            dGeoTransform[1] + (bdsRlt.iRowBeg - bdsReferance.iRowBeg) * dGeoTransform[2]
        dGeoTransform[3] = dGeoTransform[3] + (bdsRlt.iColumnBeg - bdsReferance.iColumnBeg) *\
            dGeoTransform[4] + (bdsRlt.iRowBeg - bdsReferance.iRowBeg) * dGeoTransform[5]
        iBandNum = 1 if iRltDataType == 2 or iRltDataType == 3 or iRltDataType == 6 else iBandNumRef
        if conf.output_format == ".png":
            iBandNum = 4
        if (iRltDataType == 1 or iRltDataType == 2 or iRltDataType == 6):
            dsNew = BaseProcesses.CreateNewImage(bdsRlt.sPathName, dGeoTransform, sGeoProjectionRef, bdsRlt.iColumnRange, bdsRlt.iRowRange, iBandNum, gdal.GDT_UInt16)
        else:
            dsNew = BaseProcesses.CreateNewImage(bdsRlt.sPathName, dGeoTransform, sGeoProjectionRef, bdsRlt.iColumnRange, bdsRlt.iRowRange, iBandNum, gdal.GDT_Float32)
        
        del dtsImg_1
        del dsNew

        return iBandNum

    @staticmethod
    def GenerateResultName(bdsRlt, bdsReferance, sRslPath, iPCSType):
        '''10km间镶嵌——确定镶嵌结果影像的名称
        '''
        dtsImg_1 = gdal.Open(bdsReferance.sPathName)
        dGeoTransform = list(dtsImg_1.GetGeoTransform())
        dGeoTransform[0] = dGeoTransform[0] + (bdsRlt.iColumnBeg - bdsReferance.iColumnBeg) *\
            dGeoTransform[1] + (bdsRlt.iRowBeg - bdsReferance.iRowBeg) * dGeoTransform[2]
        dGeoTransform[3] = dGeoTransform[3] + (bdsRlt.iColumnBeg - bdsReferance.iColumnBeg) *\
            dGeoTransform[4] + (bdsRlt.iRowBeg - bdsReferance.iRowBeg) * dGeoTransform[5]
        sFileName = bdsReferance.sTimeDeail +\
            str(iPCSType) +\
            str(int(dGeoTransform[0])) +\
            str(int(dGeoTransform[3])) +\
            GridCalculate.IntToString(bdsRlt.iDataProduct, 3) +\
            "_" + conf.ID + '_' + conf.search_time
        bdsRlt.sPathName = os.path.join(sRslPath, str(iPCSType), sFileName + ".tif")
        del dtsImg_1
        return bdsRlt

    @staticmethod
    def CalculateBegAndRangeForbdsRlt(bds, bdsLeftTopGrid, bdsRightBttnGrid, iImageResolution):
        '''10km间镶嵌——根据最左上角和最右下角格网，计算结果影像的begin and range
        '''
        bdsRlt = copy.deepcopy(bds)
        lefttop_grid = GridCalculate.GetCodeOfGrid(bdsLeftTopGrid.sGridCode)
        rightbttn_grid = GridCalculate.GetCodeOfGrid(bdsRightBttnGrid.sGridCode)

        iTNumY = -(rightbttn_grid[2] - lefttop_grid[2] + (rightbttn_grid[0] - lefttop_grid[0]) * 10)
        iTNumX = rightbttn_grid[3] - lefttop_grid[3] + (rightbttn_grid[1] - lefttop_grid[1]) * 10

        bdsRlt.iRowRange = int((iTNumY + 1) * (10000 / iImageResolution))
        bdsRlt.iColumnRange = int((iTNumX + 1) * (10000 / iImageResolution))
        bdsRlt.iRowBeg = 0
        bdsRlt.iColumnBeg = 0
        bdsRlt.sGridCode = bdsLeftTopGrid.sGridCode
        return copy.deepcopy(bdsRlt)

    @staticmethod
    def CalculateRBandCBforLbds10km(lbdsBtw10km, bdsReferenceGrid, iImageResolution):
        '''10km间镶嵌——以最左上角格网序列为基准，对10km序列的每个对象重计算RowBeg,ColumnBeg
        '''
        lbdsTemp = []
        code_of_grid = GridCalculate.GetCodeOfGrid(bdsReferenceGrid.sGridCode)
        for bdsBtw10km in lbdsBtw10km:
            tmp_grid = GridCalculate.GetCodeOfGrid(bdsBtw10km.sGridCode)

            iTNumY = -(tmp_grid[2] - code_of_grid[2] + (tmp_grid[0] - code_of_grid[0]) * 10)
            iTNumX = tmp_grid[3] - code_of_grid[3] + (tmp_grid[1] - code_of_grid[1]) * 10
            bdsTemp = bdsBtw10km

            bdsTemp.iRowBeg = bdsBtw10km.iRowBeg + iTNumY * (10000 / iImageResolution)
            bdsTemp.iColumnBeg = bdsBtw10km.iColumnBeg + iTNumX * (10000 / iImageResolution)
            bdsTemp.sGridCode = bdsReferenceGrid.sGridCode
            lbdsTemp.append(bdsTemp)
        return lbdsTemp



    @staticmethod
    def JudgeResultDataType(iDataProduct, iModelId):
        '''根据原始数据产品类型和模型，确定最终结果的数据类型
           DataType: 
           1—— UINT16  +  n个波段
           2—— UINT16  +  1个波段
           3—— FLOAT   +  1个波段
           4—— FLOAT   +  n个波段
           5—— INT16  +  n个波段
           6—— INT16  +  1个波段
        '''
        if (iModelId == 0):
            if (iDataProduct == 1 or iDataProduct == 11 or iDataProduct == 21 or iDataProduct == 13 or iDataProduct == 23 or iDataProduct == 4 or iDataProduct == 994):
                iRltDataType = 1
            elif iDataProduct == 3:
                iRltDataType = 3
            elif iDataProduct == 9:
                iRltDataType = 6
            else:
                iRltDataType = 3
        else:
            iRltDataType = 3
        return iRltDataType
        
    @staticmethod
    def CaculateZoneOfNewIn10km(bds1, bds2):
        '''10km内拼接——求算新ColumnBeg、RowBeg、ColumnRange、RowRange
        '''
        bds1.iColumnBeg = min(bds1.iColumnBeg, bds2.iColumnBeg)
        bds1.iRowBeg = min(bds1.iRowBeg, bds2.iRowBeg)
        bds1.iColumnRange = max((bds1.iColumnBeg + bds1.iColumnRange), (bds2.iColumnBeg + bds2.iColumnRange)) - bds1.iColumnBeg
        bds1.iRowRange = max((bds1.iRowBeg + bds1.iRowRange), (bds2.iRowBeg + bds2.iRowRange)) - bds1.iRowBeg
        return bds1

    @staticmethod
    def ReNameIn10km(bdsRlt, iModelId, sRslPath):
        '''10km内拼接——修改结果影像的命名
        '''
        bdsR = bdsRlt
        sFilename = os.path.basename(bdsRlt.sPathName)
        iMarkPixelRang = DataStruct.JudgeLengthOfPixelRang(sFilename)
        sFilename_f = sFilename[0: 14]
        sFilename_l = sFilename[14 + iMarkPixelRang * 4: ]
        IntToString = CoordinateAndProjection.IntToString
        sFilename = sFilename_f + IntToString(bdsRlt.iColumnBeg, iMarkPixelRang)\
            + IntToString(bdsRlt.iRowBeg, iMarkPixelRang)\
            + IntToString(bdsRlt.iColumnRange, iMarkPixelRang)\
            + IntToString(bdsRlt.iRowRange, iMarkPixelRang)\
            + sFilename_l

        if (iModelId != 0):
            sFilename_f = sFilename[0: 17 + iMarkPixelRang *4]
            sFilename_l = sFilename[20 + iMarkPixelRang *4: ]
            sFilename = sFilename_f + IntToString(iModelId, 3) + sFilename_l
        bdsR.sPathName = os.path.join(sRslPath, "temp", sFilename)
        return bdsR

    @staticmethod
    def ModelOfNDVI(bdsRlt, lbdsBeDeal, iBandRed, iBandIfd):
        '''未完成
        '''
        pass

    @staticmethod
    def StichingIn10km(lbds, iDataProduct, iModelId, sRslPath):
        '''接收符合拼接条件的某10公里格网内的影像序列（如tif影像序列）、拼接结果的存储路径，组织10km内的拼接逻辑
           1 求算新影像的ColumnBeg、RowBeg、ColumnRange、RowRange
           2 确定新影像的其他参数(只剩下pathname,其他在1中已经改了) --bdsRlt
           3 根据模型id,创建对应的新影像                            --datasetRlt
           4 根据模型id,选择对应的处理函数                          --bdsRlt
           NDVI未完成
        '''
        iRltDataType = Stitching.JudgeResultDataType(iDataProduct, iModelId)
        bdsRlt = copy.deepcopy(lbds[0])
        if len(lbds) > 1:
            for bds in lbds[1 : ]:
                bdsRlt = Stitching.CaculateZoneOfNewIn10km(bdsRlt, bds)
            bdsRlt = Stitching.ReNameIn10km(bdsRlt, iModelId, sRslPath)

            if iModelId == 0:
                iBandNum = Stitching.CreateResultImg_2(bdsRlt, lbds[0], iRltDataType)
                Stitching.ModelOfIntegrate_3(bdsRlt, lbds, iRltDataType, iBandNum)
            elif iModelId == 1:
                iBandNum = Stitching.CreateResultImg_2(bdsRlt, lbds[0], iRltDataType)
                if (iDataProduct == 1):
                    Stitching.ModelOfNDVI(bdsRlt, lbds, 3, 4)
        else:
            if (iModelId == 0):
                bdsRlt = Stitching.ReNameIn10km(bdsRlt, iModelId, sRslPath)
                iBandNum = Stitching.CreateResultImg_2(bdsRlt, lbds[0], iRltDataType)
                Stitching.ModelOfIntegrate_3(bdsRlt, lbds, iRltDataType, iBandNum)
            elif iModelId == 1:
                bdsRlt = Stitching.ReNameIn10km(bdsRlt, iModelId, sRslPath)
                iBandNum = Stitching.CreateResultImg_2(bdsRlt, lbds[0], iRltDataType)
                if (iDataProduct == 1):
                    Stitching.ModelOfNDVI(bdsRlt, lbds, 3, 4)
        return bdsRlt