import hera_librarian
import os
import psutil
import sys
import re


def file2jd(zenuv):
    return re.findall(r'\d+\.\d+', zenuv)[0]


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


def uploader(site, files):
    """
    Upload a list of files to the librarian

    Parameters
    ----------
    site: string
        name of librarian site
    files: list
        list of local files to upload
    root_paths: list
        list of root_paths for files (root_paths + files should give full paths
            to files)
    """
    for i, full_file in enumerate(files):
        file_size = get_size(full_file)
        store, ssh_prefix, store_path = get_recommendation(site, file_size)

        directory, filename = os.path.split(full_file)
        jd = file2jd(filename)

        mkdir_cmd = ['ssh', ssh_prefix, 'mkdir', '-p', store_path + '/' + jd]

        scp_cmd = ['scp', '-r', '-c', 'arcfour256', '-o',
                   'UserKnownHostsFile=/dev/null', '-o',
                   'StrictHostKeyChecking=no', full_file,
                   ssh_prefix + ':' + store_path + '/' + jd + '/' + filename]

        add_obs_cmd = ['ssh', ssh_prefix, 'add_obs_librarian.py', '--site ',
                       site, '--store ', store, '--store_path ',
                       store_path + '/', jd + '/' + filename]

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
