import hera_rpc

# Python binding of M&C RPCs

# create an observation
#
def create_observation(julian_date, polarization, length):
    config = get_config('.hera_mc')
    req = {'operation': 'create_observation',
        'authenticator': config['authenticator'],
        'name': name,
        'size': size,
        'md5': md5,
        'store_name': store_name}
    return do_http_post(req, config['server'])
