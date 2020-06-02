import os

armNum = {'1': 'b',
          '2': 'r',
          '3': 'n',
          '4': 'm'}


def getInfo(filepath):
    __, filename = os.path.split(filepath)
    visit = int(filename[4:10])
    arm = armNum[filename[11]]

    return visit, arm


def imgPath(butler, visit, arm):
    try:
        return butler.getUri('calexp', arm=arm, visit=visit)
    except:
        return False
