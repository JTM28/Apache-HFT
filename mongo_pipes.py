

def PIPE_1x1(exchange, _dtype):

    return [{
        '$match' : {'ns.db' : str(exchange).upper() + '-' + str(_dtype).upper()}}]



def PIPE_2x1(exchanges, _dtype):
    print('HERE')
    assert len(exchanges) == 2
    attach = '-' + str(_dtype).upper()

    return [{
        '$match' : {'$or' : [{'ns.db' : str(exchanges[0]) + str(attach)}, {'ns.db' : str(exchanges[1]) + str(attach)}]}
    }]



def PIPE_3x1(exchanges, _dtype):
    assert len(exchanges) == 3
    attach = '-' + str(_dtype).upper()

    return ([{
        '$match': {'$or': [
            {'ns.db': str(exchanges[0]) + str(attach)},
            {'ns.db': str(exchanges[1]) + str(attach)},
            {'ns.db': str(exchanges[2]) + str(attach)}]}}])



def PIPE_4x1(exchanges, _dtype):
    assert len(exchanges) == 4
    attach = '-' + str(_dtype).upper()

    return ([{
        '$match': {'$or': [
            {'ns.db': str(exchanges[0]) + str(attach)},
            {'ns.db': str(exchanges[1]) + str(attach)},
            {'ns.db': str(exchanges[2]) + str(attach)},
            {'ns.db': str(exchanges[3]) + str(attach)}]}}])



def PIPE_5x1(exchanges, _dtype):
    assert len(exchanges) == 5
    attach = '-' + str(_dtype).upper()

    return [{
        '$match': {'$or': [
            {'ns.db': str(exchanges[0]) + str(attach)},
            {'ns.db': str(exchanges[1]) + str(attach)},
            {'ns.db': str(exchanges[2]) + str(attach)},
            {'ns.db': str(exchanges[3]) + str(attach)},
            {'ns.db': str(exchanges[4]) + str(attach)}]}}]



def PIPE_6x1(exchanges, _dtype):
    assert len(exchanges) == 6
    attach = '-' + str(_dtype).upper()

    return {
        '$match': {'$or': [
            {'ns.db': str(exchanges[0]) + str(attach)},
            {'ns.db': str(exchanges[1]) + str(attach)},
            {'ns.db': str(exchanges[2]) + str(attach)},
            {'ns.db': str(exchanges[3]) + str(attach)},
            {'ns.db': str(exchanges[4]) + str(attach)},
            {'ns.db': str(exchanges[5]) + str(attach)}]}}


def get_pipe(exchanges, _dtype):
    exchanges = list(exchanges)


    n = len(exchanges)

    for i in range(n):
        exchanges[i] = str(exchanges[i]).upper()

    exchanges = list(exchanges)

    if len(exchanges) == 2:
        return PIPE_2x1(exchanges, _dtype)

    elif len(exchanges) == 3:
        return PIPE_3x1(exchanges, _dtype)

    elif len(exchanges) == 4:
        return PIPE_4x1(exchanges, _dtype)

    elif len(exchanges) == 5:
        return PIPE_5x1(exchanges, _dtype)

    elif len(exchanges) == 6:
        return PIPE_6x1(exchanges, _dtype)

    else:
        if isinstance(exchanges, str):
            return PIPE_1x1(exchanges, _dtype)
