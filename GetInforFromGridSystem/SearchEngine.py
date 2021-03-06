# coding:utf-8
#import numpy
from os import path
import os
from DataStruct import DataStruct
import conf
class SearchEngine:

    @staticmethod
    def JudgeYearDataExsit(sTkmGridPath, sDateTime):
        '''给定一个10km网格的路径和日期（8位），判断该路径下是否有与给定日期对应年份的数据
           存在，则返回加上年份后的路径列表；
        '''
        res = []
        if sDateTime == 'all' and path.exists(sTkmGridPath):
            for sYear in os.listdir(sTkmGridPath):
                objpath = os.path.join(sTkmGridPath, sYear)
                if path.exists(objpath):
                    res.append(objpath)
        else:
            sYear = sDateTime[0 : 4]
            objpath = os.path.join(sTkmGridPath, sYear)
            if path.exists(objpath):
                res.append(objpath)
        return res

    @staticmethod
    def GetObjFilenameList(sPath, sFileType):
        '''给定路径和文件格式，返回符合指定文件格式的所有文件列表
        '''
        res = []
        for root, directory, files in os.walk(sPath):
            for filename in files:
                name, suf = os.path.splitext(filename) # =>文件名,文件后缀
                if suf == sFileType:
                    res.append(os.path.join(root, filename)) # =>吧一串字符串组合成路径
        return res

    @staticmethod
    def SearchByRgDttmDtpd(sGridcode, sDatahomePath, sDateTime, iDataProduct, iCloulLevel):
        '''1-2 根据给定的某格网、日期、类型； 索引符合条件的数据 ——得到lbds序列
           若存在符合条件的数据，则返回一个有效的lbds
           否则，返回一个空的lbds
           1-1 将格网变成格网路径                                              ——ls10kmPath
           1-2 判断所查年份是否存在                                            ——ls10kmYearPath
           1-3 不存在，剔除该路径,返回一个空的lbds(即最终结果不包含该10km格网) ——ls10kmPath 
           1-4 存在，根据日期和数据产品得到该10km格网下所有符合条件的bds的序列 ——l10kmlbds     
        '''
        sTkmPath = sGridcode
        lsFilePathname = []

        sTkmPath = os.path.join(sDatahomePath, sTkmPath)

        sRsltOfJdfYearData = SearchEngine.JudgeYearDataExsit(sTkmPath, sDateTime)
        res = {}
        if len(sRsltOfJdfYearData) != 0:
            for RsltOfJdfYearData in sRsltOfJdfYearData:
                lsFilePathname = SearchEngine.GetObjFilenameList(RsltOfJdfYearData, ".tif")
                if len(lsFilePathname) != 0:
                    lBds_ = DataStruct.GetBasicDataInforList(lsFilePathname)
                    for lBd in lBds_:
                        if (lBd.iDataProduct == iDataProduct and lBd.iCloudLevel <= iCloulLevel) and (lBd.sTimeDeail == sDateTime or sDateTime == 'all'):
                            if lBd.sTimeDeail not in res.keys():
                                res[lBd.sTimeDeail] = [lBd]
                            else:
                                res[lBd.sTimeDeail].append(lBd)
        return res
