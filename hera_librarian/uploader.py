import hera_librarian
import os
import psutil
import sys


def get_size(file):
    if not os.path.exists(file):
        raise OSError('file: ' + file + ' does not exist')

    if os.path.isdir(file):
        file_size = 0
        for dirpath, dirnames, filenames in os.walk(file):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                file_size += os.path.getsize(fp)
    else:
        statinfo = os.stat(file)
        file_size = statinfo.st_size

    return file_size


def get_recommendation(site, file_size):
    client = hera_librarian.LibrarianClient(site)
    store_dict = client.recommended_store(file_size)
    path = store_dict['store']['path_prefix']
    store = store_dict['store']['name']
    ssh_prefix = store_dict['store']['ssh_prefix']

    return store, ssh_prefix, path


def uploader(site, files):
    """
    Upload a list of file to the librarian

    Parameters
    ----------
    site: string
        name of librarian site
    files: list
        list of local files to upload
    """
    for file in files:
        file_size = get_size(file)
        store, ssh_prefix, path = get_recommendation(site, file_size)

        head0, filename = os.path.split(file)
        head1, jd = os.path.split(head0)
        lib_file = os.path.join(jd, filename)

        scp_cmd = ['scp', '-r', '-c', 'arcfour256', '-o',
                   'UserKnownHostsFile=/dev/null', '-o',
                   'StrictHostKeyChecking=no', file,
                   ssh_prefix + ':' + path + '/' + lib_file]
        add_obs_cmd = ['ssh', ssh_prefix, 'add_obs_librarian.py', '--site ',
                       site, '--store', store, path + '/' + lib_file]
        print ' '.join(add_obs_cmd)

        p = psutil.Popen(scp_cmd)
        p = psutil.Popen(add_obs_cmd)


def main():
    arg_list = sys.argv
    site = arg_list[1]
    files = arg_list[2:]

    uploader(site, files)

if __name__ == '__main__':
    main()
