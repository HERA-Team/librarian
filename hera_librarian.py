# Python binding of Librarian RPCs

import hera_rpc

def get_config(site_name) {
    config = get_config('.hera.cfg')
    try:
        return config.sites[site_name]
    except IndexError:
        return none

def create_observation(site_name, obs_id, julian_date, polarization, length):
    site = get_site(site_name)
    if site == none:
        return error_struct("no such site")
    req = {'operation': 'create_observation',
        'authenticator': config['authenticator'],
        'id': obs_id,
        'julian_date': julian_date,
        'polarization': polarization,
        'length': length}
    return do_http_post(req, site)

def create_file(site_name, store_name, file_name, type, obs_id, size, md5):
    site = get_site(site_name)
    if site == none:
        return error_struct("no such site")
    req = {'operation': 'create_file',
        'authenticator': config['authenticator'],
        'store_name': store_name,
        'file_name': file_name,
        'obs_id': obs_id,
        'size': size,
        'md5': md5
    }
    return do_http_post(req, site)
