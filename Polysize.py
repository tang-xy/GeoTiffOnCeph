from osgeo import gdal

def pixel2geo(geotrans, px, py):
    ulX = geotrans[0]
    ulY = geotrans[3]
    pixelWidth = geotrans[1]
    pixelHeight = geotrans[5]
    rotate1 = geotrans[2]
    rotate2 = geotrans[4]

    Xgeo = ulX + px * pixelWidth + py * rotate1
    Ygeo = ulY + px * rotate2 + py * pixelHeight

    return Xgeo, Ygeo

def get_poly(path):
    dataset1 = gdal.Open(path)
    im_geotrans = dataset1.GetGeoTransform()
    im_width = dataset1.RasterXSize
    im_height = dataset1.RasterYSize
    x2, y2 = pixel2geo(im_geotrans, im_width, im_height)
    return im_geotrans[0], im_geotrans[3], x2, y2