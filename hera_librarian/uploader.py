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


def bash_command(command):
    """
    Issue a command to the bash shell, wait until command finishes before
        returning, if there's an error then raise and exception with the
        command that was called.

    Parameters
    ----------
    command: list
        all the elements of the command as seperate items in a list
            (ie the command will be the list with spaces inserted between
            the elements)
    """
    p = psutil.Popen(command)
    if p.wait():
        raise Exception('bash command failed, command was: ',
                        ' '.join(command))


def uploader(site, files, paths):
    """
    Upload a list of file to the librarian

    Parameters
    ----------
    site: string
        name of librarian site
    files: list
        list of local files to upload
    paths: list
        list of paths for files (paths + files should be full paths to files)
    """
    if len(paths) != 1 and len(paths) != len(files):
        raise ValueError('paths must be a list of length 1 or the same ' +
                         'length as files')

    for i, file in enumerate(files):
        if len(paths) == 1:
            full_filepath = paths[0] + file
        else:
            full_filepath = paths[i] + file

        file_size = get_size(full_filepath)
        store, ssh_prefix, path = get_recommendation(site, file_size)

        dir_part, filename = os.path.split(file)

        mkdir_cmd = ['ssh', ssh_prefix, 'mkdir', '-p', path + '/' + dir_part]

        scp_cmd = ['scp', '-r', '-c', 'arcfour256', '-o',
                   'UserKnownHostsFile=/dev/null', '-o',
                   'StrictHostKeyChecking=no', full_filepath,
                   ssh_prefix + ':' + path + '/' + file]
        add_obs_cmd = ['ssh', ssh_prefix, 'add_obs_librarian.py', '--site ',
                       site, '--store ', store, '--store_path ', path + '/',
                       file]
        print ' '.join(add_obs_cmd)

        bash_command(mkdir_cmd)
        bash_command(scp_cmd)
        bash_command(add_obs_cmd)


def main():
    arg_list = sys.argv
    site = arg_list[1]
    files = arg_list[2:]

    uploader(site, files)

if __name__ == '__main__':
    main()
