def translator_func(data, identifier, times):

    def attrSetterTwrRtls():
        def setattrWrapper(val, newTimes):
            ret = []
            for i in range(1, 10):
                if f'tagsettings.twr.target{i}' in val and f'status.twr.result{i}' in val:
                    ret.append({'coll' : 'twr', 'id' : 'tag.'+str(identifier), 'attr' : 'twrUniqueIdAndMeter', 'data' : {'value': [val[f'tagsettings.twr.target{i}'], val[f'status.twr.result{i}'] / 1000.0], 'times': newTimes}})
                    # Note: no unique ID resolution (there is LoLaN ID in patch)
            return ret
        return setattrWrapper

    # LoLaN variable translator definitions
    switcher = {
        'status.twr.inform_c': attrSetterTwrRtls()
    }

    # translating procedure
    for k, setterFunc in switcher.items():
        if k in data:
            yield setterFunc(data[k], times)