from collections import namedtuple
from fractions import Fraction as frac
import copy
import time
import json


# general "constants"
MEASUREMENT_TIME_PICOSEC = 1000000     # multiplier to convert measurement time to picosecond
MEASUREMENT_TIME_SEC = frac(MEASUREMENT_TIME_PICOSEC, 1000000000000)     # multiplier to convert measurement time to seconds (floating-point)
MEASUREMENT_TIME_TOONEW_LIMIT_SEC = 2.0    # tolerance for bad future timestamp detection: limit is current time plus this value to avoid false alerts due to unsynchronized clocks

# buffering parameters
TSDBUF_DATA_AGE_LIMIT_SEC = 3600      # data age limit (seconds) for buffering: data older than this will be discarded
TSDBUF_CHUNK_SIZE_MIN = 10     # minimum target chunk size for long-term database
TSDBUF_CHUNK_CLOSE_TIMEOUT_NORMAL_SEC = 60    # chunk close timeout (seconds) when the data count is at least TSDBUF_CHUNK_SIZE_MIN
TSDBUF_CHUNK_CLOSE_TIME_LIMIT_SEC = 600    # chunk close time limit (seconds), close chunk after this time even the data count is less than TSDBUF_CHUNK_SIZE_MIN

# misc parameters
TSDBUF_AVOID_DUP_BUFFER_CLEANUP_INTERVAL_SEC = 60   # interval (seconds) for removing too old items from the duplication avoidance buffer

# variables for dummy time sync: we save this two at the same time
timesyncMeasurementTime = 0
timesyncTickCount = 0.0

# variables for timing
tsdbufAvoidDupBufferCleanupLastTime = 0.0
tsdbufChunkCollectionStartTime = float('inf')

# variables for DCM updating
tsdLatestMeasTimeStorage = {}   # storage for the measurement time of latest TSD data for each device and LoLaN variable

# variables for buffering
chunkBufferRecordType = namedtuple('chunkBufferRecordType', ['id', 'field', 'values', 'times'])
tsdbufChunkBuffer = []   # buffer for creating chunks for long-term database; elements: chunkBufferRecordType
tsdbufAvoidDupBuffer = set()   # buffer to help avoid duplicates in output data; elements: (idCompound, measTime)


def tsdbufTimeSync(times):   # synchronize measurement time for TSD buffering

    global timesyncTickCount
    global timesyncMeasurementTime

    if 'measurement' in times and times['measurement']:
            timesyncTickCount = time.time()
            timesyncMeasurementTime = times['measurement']

def measurementTimeToTickCount(measTime):   # convert measurement time to TickCount using the internal synchronization
    return timesyncTickCount - (timesyncMeasurementTime - measTime) * MEASUREMENT_TIME_SEC

def notTooOld(measTime):    # check whether the given measurement time is not too old to fit in the buffering window
    t = measurementTimeToTickCount(measTime)
    return t + TSDBUF_DATA_AGE_LIMIT_SEC >= time.time()

def notTooNew(measTime):    # check whether the given measurement time is not too new (is in future)
    t = measurementTimeToTickCount(measTime)
    return t < time.time() + MEASUREMENT_TIME_TOONEW_LIMIT_SEC

def tsdLatestCheckUpdate(idCompound, measTime):    # check whether the TSD data with this time is the latest and update latest time if needed
    if idCompound in tsdLatestMeasTimeStorage and tsdLatestMeasTimeStorage[idCompound] >= measTime:   # stored yet and the data to check is older
        return False
    tsdLatestMeasTimeStorage[idCompound] = measTime   # does not exist yet, or newer
    return True

def tsdbufAddRecord(idCompound, values, times):   # add record to the TSD buffer avoiding duplicates

    global tsdbufChunkCollectionStartTime
    measTime = times['measurement']
    dupBufRecord = (idCompound, measTime)

    if notTooOld(measTime):
        if dupBufRecord not in tsdbufAvoidDupBuffer:   # this data is not buffered yet
            tsdbufAvoidDupBuffer.add(dupBufRecord)   # add to duplicate avoidance buffer
            if not tsdbufChunkBuffer:   # empty yet
                tsdbufChunkCollectionStartTime = time.time()   # save time as chunk collection start time
            record = chunkBufferRecordType(idCompound[0], idCompound[1], values, times)
            tsdbufChunkBuffer.append(record)   # add to chunk buffer

def tsdbufWriteHistory():   # write chunk(s) to the history

    global tsdbufChunkBuffer
    global tsdbufChunkCollectionStartTime

    if tsdbufChunkBuffer and time.time() >= tsdbufChunkCollectionStartTime + TSDBUF_CHUNK_CLOSE_TIMEOUT_NORMAL_SEC:   # some data exists and normal timeout
        if len(tsdbufChunkBuffer) >= TSDBUF_CHUNK_SIZE_MIN or time.time() >= tsdbufChunkCollectionStartTime + TSDBUF_CHUNK_CLOSE_TIME_LIMIT_SEC:   # enough data or time limit
            # create and write chunks for each field
            auxStruct = {}
            for record in tsdbufChunkBuffer:   # create outStruct grouping by field name and device identifier
                if record.field not in auxStruct:
                    auxStruct[record.field] = {}
                if record.id not in auxStruct[record.field]:
                    auxStruct[record.field][record.id] = []
                auxStruct[record.field][record.id].append( {'dcmTime' : record.times['measurement'], 'measurementTime' : record.times['measurement'], 'sensorsetbufferTime' : record.times['sensorsetbuffer'], 'value' : record.values} )
            for field, fieldEntries in auxStruct.items():   # handle every field separately
                outStruct = [{'id': identifier, 'changes': changes} for identifier, changes in fieldEntries.items()]
                # FIXME redis.push(json.dumps(outStruct), field)   # write chunk
            # finalize
            tsdbufChunkBuffer.clear()
            tsdbufChunkCollectionStartTime = float('inf')   # reset start time

def tsdbufAvoidDupBufferCleanUp():   # remove too old items
    global tsdbufAvoidDupBuffer
    global tsdbufAvoidDupBufferCleanupLastTime
    if time.time() - tsdbufAvoidDupBufferCleanupLastTime >= TSDBUF_AVOID_DUP_BUFFER_CLEANUP_INTERVAL_SEC:
        tsdbufAvoidDupBuffer = set(filter(lambda item: notTooOld(item[1]), tsdbufAvoidDupBuffer))   # filter too old data from buffer
        tsdbufAvoidDupBufferCleanupLastTime = time.time()

def tsdAbsoluteTime2measurementTime(time):   # supply time in picoseconds!
    return time // MEASUREMENT_TIME_PICOSEC   # measurement time is UTC with epoch 1970.01.01. in microseconds, TSD absolute time should be also this type to avoid leap second calculcation

def tsdProcessor(attrSetter, idCompound, buffering, transform = lambda x: x):   # processor function for TSD data
# param[in] attrSetter:   function(data, times)
# param[in] idCompound:   tuple(device identifier as a single number, data field name)
# param[in] buffering:    boolean, set True to make buffering and history input for this data
# param[in] transform:    function to transform data

    def internalFunc(vals, times):
        xxout = [{'coll' : 'dummy'}]
        timestampExists = 'timestamp' in vals   # timestamp definition exists (-> timestamp available for data)
        measTimeExists = 'measurement' in times and type(times['measurement']) in [int, float]   # measurement time exists (BDCL found RxPacket for that packet)
        if timestampExists:
            timestampRelative = vals['timestamp']['absolute or relative']
            tsdTimeMultiplier = {
                'picoseconds':  1,
                'nanoseconds':  1000,
                'microseconds': 1000000,
                'milliseconds': 1000000000,
                'seconds':      1000000000000,
                'minutes':      60000000000000
            }[vals['timestamp']['unit']]
            if measTimeExists:
                measTimePicosec = times['measurement'] * MEASUREMENT_TIME_PICOSEC
                if timestampRelative == 'relative':    # relative timestamp (relative to a random time point, last is approx. measurementTime)
                    firstTsPicosec = measTimePicosec - tsdTimeMultiplier * vals['data'][-1]['timestamp']   # compute absolute time value of first relative timestamp (FIXME: modify to maximum search)
        for data in vals['data']:
            newtimes = copy.deepcopy(times)
            newMeasTime = False
            if timestampExists:
                if timestampRelative == 'relative':    # relative timestamp (relative to a random time point, last is approx. measurementTime)
                    if measTimeExists:   # measurement time exists
                        newtimes['measurement'] = (firstTsPicosec + tsdTimeMultiplier * data['timestamp']) // MEASUREMENT_TIME_PICOSEC   # compute measurement time
                        newMeasTime = True
                elif timestampRelative == 'relative (reversed)':    # relative timestamp (backwards distance from measurementTime)
                    if measTimeExists:   # measurement time exists
                        newtimes['measurement'] = (measTimePicosec - tsdTimeMultiplier * data['timestamp']) // MEASUREMENT_TIME_PICOSEC   # compute measurement time
                        newMeasTime = True
                else:    # absolute timestamp
                    newtimes['measurement'] = tsdAbsoluteTime2measurementTime(tsdTimeMultiplier * data['timestamp'])   # measurement time will be the absolute time from TSD
                    newMeasTime = True
            if measTimeExists or newMeasTime:   # measurement time is available from somewhere
                newtimesMeasurement = newtimes['measurement']
                if notTooNew(newtimesMeasurement):    # eliminate bad timestamps (pointing to future)
                    if tsdLatestCheckUpdate(idCompound, newtimesMeasurement):   # is this data newer?
                        xxout.extend(attrSetter(transform(data['values']), newtimes))    # newer, or does not exist yet, update DCM
                    if buffering:
                        tsdbufAddRecord(idCompound, transform(data['values']), newtimes)
                else:   # bad timestamp
                    print("Bad measurement time for {}, value: {}, in tickCount: {}, current tickCount: {}".format(idCompound, newtimesMeasurement, measurementTimeToTickCount(newtimesMeasurement), time.time()))
        return xxout

    return internalFunc

def tsdbufProcess(times):   # process for TSD buffering, call in the redis_translator function in the translators where this module is used

    # time sync procedure
    tsdbufTimeSync(times)

    # duplicate buffer clean-up
    tsdbufAvoidDupBufferCleanUp()

    # history write process
    tsdbufWriteHistory()
