def imgPath(butler, visit, arm):
    try:
        return butler.getUri('calexp', arm=arm, visit=visit)
    except:
        return False
