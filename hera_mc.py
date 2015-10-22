# Python binding of M&C RPCs

import hera_rpc

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

def create_status(observation_id, status, current_pid, still_host, still_path, output_host, output_path):
    config = get_config('.hera_mc')
    req = {'operation': 'create_status',
        'authenticator': config['authenticator'],
        'observation_id': observation_id,
        'status': status,
        'current_pid': current_pid,
        'still_host': still_host,
        'still_path': still_path,
        'output_host': output_host,
        'output_path': output_path }
    return do_http_post(req, config['server'])
