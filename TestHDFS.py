# coding:utf-8
import sys, os


def createtif(url):
    for f in os.listdir(url):
        real_path=os.path.join(url,f)
        if os.path.isfile(real_path):
            filename, fileend = os.path.splitext(real_path)
            if fileend == '.tfw':
                tiffile = open(filename + ".tif","w+")
                tiffile.write(str([i for i in range(800000)]))
        elif os.path.isdir(real_path):
            createtif(real_path)
        else:
            print("其他情况:" + real_path)



if __name__ == "__main__":
    #if sys.argv[2] = 'upload':
    if True:
        createtif('32652(copy)/5104')