import asyncio
import aioredis
import websockets
import json
import logging
import importlib
import traceback
from collections import deque


## configuration ##
dcmPatchWsUrl = 'ws://dcm/v2/{}/patchwebsocket'   # DCM patch websockets' URL format (FIXME: ?force=true if websockets are left open)
dcmCollections = ['generalTags', 'locations', 'pairings', 'extras', 'twr', 'sclpositions']   # available DCM collections
translators = ['generaltags_v2', 'locations_v2', 'scanner_ble_v2', 'twr_v2']   # translator modules (python files)

## variables ##
doq = deque()    # data out queue
translatorsImp = []   # imported translator modules (returned by importlib)
wsObjects = {}   # websocket object storage


# redis reader task
async def redisReader(channel: aioredis.client.PubSub):
    global doq
    jsondata = {}
    async for message in channel.listen():
        if message is not None:
            try:
                if message['type'] != 'pmessage':   # filter for normal messages (not subscribe etc.)
                    continue
                jsondata = json.loads(message['data'])
            except:
                logging.warning('(redisReader) Not a valid json from BDCL.')
                jsondata = {}
            if jsondata:
                if message['channel'].decode('ascii') == '451513e9-da18-4c35-863c-877bac283863':  # SCL message
                    print(jsondata)
                    # extract data
                    unqId = jsondata.get('devId', 0)
                    uuid = jsondata.get('uuid', 'None')
                    measTime = jsondata.get('timestamp', None)
                    ssTime = jsondata.get('sensorsetbufferTime', None)
                    positions = jsondata.get('positions', None)
                    # add to queue
                    if positions is not None:
                        value = [pos['positionVector'] for pos in jsondata['positions']]
                    else:
                        value = [[0.0, 0.0, 0.0]]  # value is a list of position vectors
                    px = {
                        'coll' : 'sclpositions',
                        'id' : 'tag.' + str(unqId),
                        'attr' : 'sclProfiles/' + uuid + '/rawPositions',
                        'data' : {'value' : value, 'times' : {'measurement' : measTime, 'sensorsetbuffer' : ssTime}}
                    }
                    doq.append(px)
                else:   # BDCL message
                    # extract data
                    datamap = jsondata.get('data', None)
                    header = jsondata.get('header', {})
                    unqId = header.get('uniqId', None)
                    measTime = header.get('measTs', None)
                    ssTime = header.get('serverTs', None)
                    # error check
                    if datamap is None or unqId is None:
                        continue
                    # measurement time check
                    if measTime is None:
                        if ssTime is None:
                            logging.warning("Neither measurement time nor sensorSetBuffer time present for data from '%u'.", unqId)
                        else:
                            measTime = ssTime
                            logging.debug("No measurement time for this data from '%u', assuming that sensorSetBuffer time is also the measurement time.", unqId)
                    # invoke translator and add data to "data out queue"
                    for module in translatorsImp:
                        try:
                            for outList in module.translator_func(datamap, unqId, {'measurement' : measTime, 'sensorsetbuffer' : ssTime}):   # FIXME
                                for do in outList:
                                    doq.append(do)
                        except BaseException as e:
                            logging.error(
                                "Exception '%s' with message '%s' in translator module '%s'\n%s",
                                type(e).__name__, str(e), module.__name__, traceback.format_exc()
                            )


# task to reconnect a websocket
async def wsReconnectTask(coll):
    global wsObjects

    url = dcmPatchWsUrl.format(coll)
    while coll not in wsObjects:
        await asyncio.sleep(1.0)   # 1 sec
        logging.warning("Trying to reconnect to websocket '%s' ...", url)
        try:
            ws = await websockets.connect(url)
            wsObjects.update({coll : ws})
            logging.info("Successfully reconnected to websocket '%s'", url)
        except:
            logging.error("Cannot reconnect to websocket '%s'", url)


# MAIN
async def main():
    global doq
    global wsObjects

    # initialize redis reader
    redis = aioredis.from_url('redis://bdcl')
    redisPubsub = redis.pubsub()
    await redisPubsub.psubscribe('451513e9-da18-4c35-863c-877bac28386*')
    redisTask = asyncio.create_task(redisReader(redisPubsub))

    # connect to DCM patch websockets
    for coll in dcmCollections:
        url = dcmPatchWsUrl.format(coll)
        try:
            ws = await websockets.connect(url)
            wsObjects.update({coll : ws})
        except:
            logging.error("Cannot connect to websocket '%s'", url)

    logging.info('Loop starting...')

    # send loop
    if wsObjects:   # successfully connected to at least one websocket
        while wsObjects:
            if not doq:   # queue is empty
                await asyncio.sleep(0.1)
            else:   # item(s) in queue
                item = doq.popleft()
                if item['coll'] in wsObjects:   # websocket for this collection is available
                    ws = wsObjects[item['coll']]
                    patch = {
                        'op' : 'replace',
                        'path' : '/{}/{}'.format(item['id'], item['attr']),
                        'value' : item['data']['value'],
                        'times': item['data']['times']
                    }
                    wsData = json.dumps([patch])
                    try:
                        await ws.send(wsData)
                        logging.info("Data sent to websocket '%s': %s", item['coll'], wsData)
                    except:
                        logging.error("Cannot send to websocket of collection '%s', will be removed from list now.", item['coll'])
                        wsObjects.pop(item['coll'])
                        asyncio.create_task(wsReconnectTask(item['coll']))  # try to reconnect
                else:
                    logging.warning("Websocket for collection '%s' is not available.", item['coll'])
    else:
        logging.critical('Cannot connect to any websockets at all.')
    
    # finalize FIXME make these run on termination
    for coll, ws in wsObjects.items():
        await ws.close()
    await redisPubsub.unsubscribe()
    await redisPubsub.close()
    await redisTask.cancel()


if __name__ == '__main__':    
    # logging
    logging.basicConfig(level = logging.INFO)
    # import translator modules
    for module in translators:
        translatorsImp.append(importlib.import_module(module))
    # asyncio
    asyncio.run(main())
