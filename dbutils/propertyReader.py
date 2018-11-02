import configparser
import os
config = configparser.RawConfigParser()
def getPropInstance( sdirPath, sfileName):

    fileDir = os.path.dirname(sdirPath)
    filename = os.path.join(fileDir, sfileName)
    config.read(filename)
    return config

def getPropDict(item):
    return dict(config.items(item))
