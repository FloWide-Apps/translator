import tsdbuf_v2


def translator_func(data, identifier, times):

    def attrSetter(attr):
        return lambda val, newTimes: ([{'coll' : 'generalTags', 'id' : identifier, 'attr' : attr, 'data' : {'value': val, 'times': newTimes}}])        

    def chargingStatusSetter(xData, xTimes):
        externalPowerAvailable = False
        isCharging = False
        if xData == 1:
            externalPowerAvailable = True
            isCharging = True
        elif xData == 2:
            externalPowerAvailable = True
        return [
            {'coll' : 'generalTags', 'id' : identifier, 'attr' : 'externalPowerAvailable', 'data' : {'value': externalPowerAvailable, 'times': xTimes}},
            {'coll' : 'generalTags', 'id' : identifier, 'attr' : 'isCharging', 'data' : {'value': isCharging, 'times': xTimes}}
        ]

    # the dataBits parameter is the bit depth for data, which corresponds to an acceleration range of +/-2g
    def accelTransform(dataBits):
        def internalFunc(xData):
            mul = 40.0 / (2 ** dataBits)   # assume g as 10 m/sec2
            return [item * mul for item in xData]
        return internalFunc

    def accelDataProcessor(xData, xTimes):
        if 'status.lastaccel.acc_data_tsd' in xData:   # TSD format
            return tsdbuf_v2.tsdProcessor(attrSetter('accelerometerA'), (identifier, 'accelerometerA'), True, accelTransform(xData['status.lastaccel.databits']))(xData['status.lastaccel.acc_data_tsd'], xTimes)
        elif 'status.lastaccel.x' in xData and 'status.lastaccel.y' in xData and 'status.lastaccel.z' in xData:   # normal format
            cd = [xData['status.lastaccel.x'], xData['status.lastaccel.y'], xData['status.lastaccel.z']]
            return attrSetter('accelerometerA')(accelTransform(xData['status.lastaccel.databits'])(cd), xTimes)

    # TSD buffering process
    tsdbuf_v2.tsdbufProcess(times)

    # LoLaN variable translator definitions
    switcher = {
        'status.battery.level': attrSetter('batteryVoltage'),
        'status.battery.charging': chargingStatusSetter,
        'standard.power.battery_voltage': attrSetter('batteryVoltage'),
        'standard.power.external_voltage': attrSetter('externalVoltage'),
        'status.temperature': attrSetter('temperatureC'),
        'status.lastaccel.acc_raw_packed': accelDataProcessor,
        'status.distance_tsd': tsdbuf_v2.tsdProcessor(attrSetter('distanceM'), (identifier, 'distanceM'), True, lambda x: x / 1000),
        'status.pressure_tsd': tsdbuf_v2.tsdProcessor(attrSetter('pressurePa'), (identifier, 'pressurePa'), True)
    }

    # translating procedure
    for k, setterFunc in switcher.items():
        if k in data:
            yield setterFunc(data[k], times)
