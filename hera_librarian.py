# Python binding of Librarian RPCs

import hera_rpc

def create_observation(id, julian_date, polarization, length_days):
    config = get_config('.hera_librarian')
    req = {'operation': 'create_observation',
        'authenticator': config['authenticator'],
        'id': id,
        'julian_date': julian_date,
        'polarization': polarization,
        'length_days': length_days}
    return do_http_post(req, config['server'])

def create_file(name, observation_id, size, path, url, md5, store_name):
    config = get_config('.hera_librarian')
    req = {'operation': 'create_file',
        'authenticator': config['authenticator'],
        'name': name,
        'observation_id': observation_id,
        'size': size,
        'path': path,
        'url': url,
        'md5': md5,
        'store_name': store_name}
    return do_http_post(req, config['server'])
