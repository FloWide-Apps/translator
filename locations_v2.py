import tsdbuf_v2


def gps_transform(data):   # input is [lat, long] in format 4729.25 means 47deg 29.25', output is in format 47.4875 means 47.4875deg
    degreesLat, minutesLat = divmod(data[0], 100)
    degreesLong, minutesLong = divmod(data[1], 100)
    return [degreesLat + minutesLat / 60, degreesLong + minutesLong / 60]


# input is [lat, long, ex]
#   format for lat, long: 472804724 means 47deg 28.04724', output is in format 47.4875 means 47.4875deg
#   format for ex: <MSB><8-bit zeros><16-bit HDOP x100><8-bit speed in km/h><LSB>
def gps_transform_ex(data):
    degreesLat, minutesLat = divmod(data[0], 10000000)
    degreesLong, minutesLong = divmod(data[1], 10000000)
    hdop = ((data[2] >> 8) & 0xFFFF) / 100
    quality = 1.0 if hdop <= 1.0 else 1 / hdop
    velo = (data[2] & 0xFF)
    return [[degreesLat + minutesLat / 6000000, degreesLong + minutesLong / 6000000], quality, [velo / 3.6, 0, 0]]


def translator_func(data, identifier, times):

    def attrSetter(attr, transform = lambda e: e):
        return lambda val, newTimes: ([{'coll' : 'locations', 'id' : 'tag.'+str(identifier), 'attr' : attr, 'data' : {'value': transform(val), 'times': newTimes}}])

    def gpsDataExMultiSetter(val, newTimes):
        return [
            {'coll' : 'locations', 'id' : 'tag.'+str(identifier), 'attr' : 'gpsPosition', 'data' : {'value': val[0], 'times': newTimes}},
            {'coll' : 'locations', 'id' : 'tag.'+str(identifier), 'attr' : 'quality', 'data' : {'value': val[1], 'times': newTimes}},
            {'coll' : 'locations', 'id' : 'tag.'+str(identifier), 'attr' : 'velocity', 'data' : {'value': val[2], 'times': newTimes}}
        ]

    # TSD buffering process
    tsdbuf_v2.tsdbufProcess(times)

    # LoLaN variable translator definitions
    switcher = {
        'status.lastaccel.ismoving': attrSetter('isMoving', bool),
        'status.gpsdata_tsd': tsdbuf_v2.tsdProcessor(attrSetter('gpsPosition'), (identifier, 'gpsPosition'), True, gps_transform),
        'status.gps.gpsdata_tsd': tsdbuf_v2.tsdProcessor(attrSetter('gpsPosition'), (identifier, 'gpsPosition'), True, gps_transform),
        'status.gps.gpsdata_ex_tsd': tsdbuf_v2.tsdProcessor(gpsDataExMultiSetter, (identifier, 'do not care'), False, gps_transform_ex)
    }

    # translating procedure
    for k, setterFunc in switcher.items():
        if k in data:
            yield setterFunc(data[k], times)
