import binascii
import tsdbuf_v2
import copy
import json
import time
from shapely.geometry import Polygon
from shapely.ops import unary_union


dummyScanCounter = 0    # to generate scan counter value for BLE scan data
deviceTickCountData = {}    # for tick count to measurement time conversion

BLERTLS_CONFIG_FILE = '/data/shared_files/ble_rtls.conf'   # BLE RTLS config file
BLERTLS_CONFIG_REREAD_INTERVAL = 5   # re-read interval for config file (sec)
BLERTLS_INRANGE_TIMEOUT = 3   # beacon is in range timeout (sec)
bleRtlsConfigReadLastTime = 0    # last BLE RTLS config file read time
bleRtlsConfig = {}
bleTagsLastTimeInRange = {}


def translator_func(data, identifier, times):

    def attrSetter(attr):
        return lambda val, newTimes: ([{'coll' : 'pairings', 'id' : 'tag.'+str(identifier), 'attr' : attr, 'data' : {'value': val, 'times': newTimes}}])

    def attrSetterX(attr):
        return lambda val, newTimes: ({'coll' : 'pairings', 'id' : 'tag.'+str(identifier), 'attr' : attr, 'data' : {'value': val, 'times': newTimes}})

    # set "barCode" attribute and compute position
    def attrSetterBleRtls():
        def setattrWrapper(val, newTimes):
            global bleRtlsConfig
            global bleRtlsConfigReadLastTime
            global bleTagsLastTimeInRange            
            # set attribute
            xxout = [{'coll' : 'pairings', 'id' : 'tag.'+str(identifier), 'attr' : 'barCode', 'data' : {'value': val, 'times': newTimes}}]
            # read BLE RTLS config
            if time.perf_counter() - bleRtlsConfigReadLastTime >= BLERTLS_CONFIG_REREAD_INTERVAL:   # re-read config
                bleRtlsConfigReadLastTime = time.perf_counter()
                bleRtlsConfig = {}
                try:
                    with open(BLERTLS_CONFIG_FILE, 'r') as f:
                        bleRtlsConfig = json.load(f)
                        f.close()
                except:
                    pass
            # do RTLS
            if identifier not in bleTagsLastTimeInRange:   # no data for this scanning device yet
                bleTagsLastTimeInRange.update({identifier : {}})
            if bleRtlsConfig and type(val) == str and len(val) >= 14 and val[12] == ':':
                # extract BLE address and RSSI
                x = val.split(':', 1)
                addr = x[0]
                rssi = int(x[1])
                # check whether this device is in range
                for bb in bleRtlsConfig['bleBeacons']:
                    if addr.upper() == bb['bleAddress'].upper():
                        if rssi >= bb['inRangeRssiLimit']:   # in range, store time
                            bleTagsLastTimeInRange[identifier].update({bb['secondaryId'] : time.perf_counter()})
                        break
                # update position
                for z in bleRtlsConfig['zones']:
                    num = 0
                    for el in z['elements']:
                        if el in bleTagsLastTimeInRange[identifier]:
                            if time.perf_counter() - bleTagsLastTimeInRange[identifier][el] <= BLERTLS_INRANGE_TIMEOUT:
                                num = num + 1
                    if num == len(z['elements']):  # all elements are in range
                        # compute center   !FIXME! 2D only yet
                        polys = []
                        for el in z['elements']:
                            for bb in bleRtlsConfig['bleBeacons']:
                                if bb['secondaryId'] == el:
                                    polys.append(Polygon(bb['rangeBaseArea']))
                                    break
                        if polys:
                            uc = unary_union(polys).centroid
                            output = [uc.x, uc.y, 0]
                            xxout.append({'coll' : 'locations', 'id' : 'tag.'+str(identifier), 'attr' : 'position', 'data' : {'value': output, 'times': newTimes}})
                        break
            return xxout

        return setattrWrapper

    def bleScanExtract(tsdData, newTimes):   # extract BLE scan info from dummy format TSD data

        global dummyScanCounter

        # generate dummy TSD data
        devUniqueDummyTsd = copy.deepcopy(tsdData)
        bleAddrRssiDummyTsd = copy.deepcopy(tsdData)
        dummyScanCounterDummyTsd = copy.deepcopy(tsdData)
        for i, tsdElement in enumerate(tsdData['data']):   # for each TSD element
            xdata = tsdElement['values']
            devUniqueDummyTsd['data'][i]['values'] = xdata[2];   # obtain unique ID (only devices with firmware 1.7.0 or newer), and update dummy TSD
            bleAddr25 = binascii.hexlify( bytearray([(xdata[1] >> s & 0xFF) for s in (0,8,16,24)]) )   # obtain Bluetooth address
            bleAddr01 = binascii.hexlify( bytearray([(xdata[0] >> s & 0xFF) for s in (0,8)]) )
            rssi = xdata[0] >> 16 & 0xFF   # obtain RSSI
            if rssi > 127:
                rssi -= 256
            bleAddrRssiDummyTsd['data'][i]['values'] = bleAddr25.decode() + bleAddr01.decode() + ':' + str(rssi)    # update dummy TSD with Bluetooth address and RSSI
            dummyScanCounter = (dummyScanCounter + 1) % 256   # generate dummy scan counter value
            dummyScanCounterDummyTsd['data'][i]['values'] = dummyScanCounter

        # call setter functions
        xxout = []
        xxout.extend(tsdbuf_v2.tsdProcessor(attrSetterBleRtls(), (identifier, 'barCode'), True)(bleAddrRssiDummyTsd, newTimes))   # Bluetooth address to DCM and history as 'barCode'
        xxout.extend(tsdbuf_v2.tsdProcessor(attrSetter('pairingCode'), (identifier, 'pairingCode'), True)(devUniqueDummyTsd, newTimes))   # unique ID of scanned device to DCM and history as 'pairingCode'
        xxout.extend(tsdbuf_v2.tsdProcessor(attrSetter('scanCounter'), (identifier, 'scanCounter'), False)(dummyScanCounterDummyTsd, newTimes))   # dummy scan counter only to DCM
        return xxout

    def tickCountDataAdd(tickCount, newTimes):   # store tick count data for a device

        global deviceTickCountData

        if 'measurement' in newTimes:    # measurement time exists (BDCL found RxPacket for that packet)
            if identifier in deviceTickCountData:   # data stored yet
                deviceTickCountData[identifier]['former'] = deviceTickCountData[identifier]['last']
            else:
                deviceTickCountData[identifier] = {}
            deviceTickCountData[identifier]['last'] = {'measTime': newTimes['measurement'], 'tickCount': tickCount}
        return [{'coll' : 'dummy'}]

    def measTimeCompute(lolanData, newTimes):   # compute measurement time

        global deviceTickCountData

        if 'scanstatus.scannerapp.scan_time' in lolanData:    # scan time exists
            newTimesCopy = copy.deepcopy(newTimes)
            if identifier in deviceTickCountData and 'last' in deviceTickCountData[identifier] and 'former' in deviceTickCountData[identifier]:   # have enough time points
                measTimeKnownInterval = deviceTickCountData[identifier]['last']['measTime'] - deviceTickCountData[identifier]['former']['measTime']
                tickCountKnownInterval = deviceTickCountData[identifier]['last']['tickCount'] - deviceTickCountData[identifier]['former']['tickCount']
                tickCountUnknownInterval = lolanData['scanstatus.scannerapp.scan_time'] - deviceTickCountData[identifier]['last']['tickCount']
                newTimesCopy['measurement'] = deviceTickCountData[identifier]['last']['measTime'] + int(tickCountUnknownInterval / tickCountKnownInterval * measTimeKnownInterval)   # compute measurement time
            else:   # not enough time data
                if 'measurement' in newTimesCopy:
                    del newTimesCopy['measurement']   # indicate unknown measurement time by deleting measurement time from record
            return newTimesCopy
        else:   # no scan time in container
            return newTimes    # do not change the measurement time

    # TSD buffering process
    tsdbuf_v2.tsdbufProcess(times)

    switcher = {
        'data.scannerapp.scanout_c': lambda val, times:    # old ScannerTag firmware
            [attrSetterX(attribute)(val[lolan], times) for attribute, lolan in [('barCode', 'data.scannerapp.scandata_single'), ('scanCounter', 'data.scannerapp.scan_cnt'), ('pairingCode', 'data.scannerapp.scan_associated_num')]],
        'scanstatus.scannerapp.scanout_c': lambda val, times:    # new general nRF tag firmware
            [attrSetterX(attribute)(val[lolan], measTimeCompute(val, times)) for attribute, lolan in [('barCode', 'scanstatus.scannerapp.scandata_single'), ('scanCounter', 'scanstatus.scannerapp.scan_cnt'), ('pairingCode', 'scanstatus.scannerapp.scan_associated_num')]],
        'status.ibutton.out_c': lambda val, times:
            [attrSetterX(attribute)(typeof(val[lolan]), times) for attribute, lolan, typeof in [('barCode', 'status.ibutton.serial', str), ('scanCounter', 'status.ibutton.seq', int)]],
        'status.blescandata_tsd': bleScanExtract,
        'status.general.tick_count': tickCountDataAdd
    }

    for k, setterFunc in switcher.items():
        if k in data:
            yield setterFunc(data[k], times)
