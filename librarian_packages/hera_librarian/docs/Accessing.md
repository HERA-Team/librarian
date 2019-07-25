# Accessing the HERA Librarian

There are three major ways to access a HERA Librarian server.

Recall that there are multiple Librarians running at different sites, so it’s
not quite fair to talk about “*the* Librarian”. But for most users, the
Librarian running at the NRAO is the one that matters.


### Table of Contents

* [Accessing over the web](#accessing-over-the-web)
* [Accessing from the command line](#accessing-from-the-command-line)
* [Accessing from inside a Python program](#accessing-from-inside-a-python-program)


## Accessing over the web

Each Librarian provides a web interface that lets you browse files, run
searches, and so on. The hope for the web interface is that it lets you Just
Do What You Want™. If you’re looking at it and there’s something that you want
to do, but can’t, [let us know](https://github.com/HERA-Team/librarian/issues)!

The Librarian web interfaces are *not* accessible over the open Web. Securing
an open web-facing service, especially one with active server code in a
language like Python, is a more-than-full-time job, and we just don’t have the
resources to do this. We obtain security by running the Librarians behind
firewalls and having you access them via SSH port forwarding. We know this is
inconvenient, but this is the best balance between security and convenience
that we have been able to find, given our limited resources.

To set up a standard NRAO Librarian port forward, please see
[the instructions on the HERA internal wiki](http://hera.pbworks.com/w/page/118774905/Librarian%3A%20Help%20for%20Users).

Once you are logged in, the main parts of the interface allow you to browse
and search the files registered with that Librarian.


## Accessing from the command line

You can also interact with a Librarian server using command-line programs. We
expect that most users will be doing this while logged into processing nodes
at NRAO, but you can use these programs from any machine that can make a
connection to the Librarian web server. Importantly, because of the way we use
SSH port forwards to connect to Librarians, *you can use these programs on
your own laptop*, if the port forward is in effect.

To make the Librarian client programs available, you need to do a standard

```
python setup.py install
```

from the [top level of this repository](..). This should install scripts like
[librarian_locate_file.py](../scripts/librarian_locate_file.py) into your
shell path.

You also need to create a file called `~/.hl_client.cfg`, which tells the
programs how they can connect to a Librarian server. Importantly, this
configuration file lets you define *multiple* Librarian servers, each with a
nickname that you choose. Pretty much every client program takes an argument
in which you specify which server you want to talk to.

The configuration file is in [JSON](http://www.json.org/) format. For accounts
at the AOC, the file should look like:

```
{
    "connections": {
        "local": {
            "url": "http://146.88.1.90:21106/",
            "authenticator": "HIDDEN-SECRET"
        }
    }
}
```

The “authenticator” field is a password so we can’t reproduce it here. As
mentioned above, you may find HERA’s connection information
[here on the HERA Wiki](http://hera.pbworks.com/w/page/118774905/Librarian%3A%20Help%20for%20Users).

On your own laptop, to work with a port forward, your file should look like:

```
{
    "connections": {
        "aoc": {
            "url": "http://localhost:21106/",
            "authenticator": "HIDDEN-SECRET"
        }
    }
}
```

If everything is working and you believe that you have a working connection to
the NRAO Librarian, the following command should print out an SSH-friendly file path:

```
librarian_locate_file.py local zen.2458030.17452.auto_specs.png
```

Above we have assumed that your connection to the NRAO Librarian is named
`local` in your `~/.hl_client.cfg` file.


## Accessing from inside a Python program

It is also possible to talk to Librarians *programmatically*, from inside a
Python program of your own creation.

If you install and configure the Librarian command-line programs as above,
you’re already equipped to do so. Those programs use a Python module called
`hera_librarian` that lets you issue commands to the server.

In brief, you create an object of type `hera_librarian.LibrarianClient`, and
call methods on it. These methods map *very* directly onto API calls exposed
by the Librarian web server.

Sorry: there’s no clean documentation right now. You can read the source code
to the command-line programs to see what they do, and read
[the source code to the client module](../hera_librarian/__init__.py) to see
what APIs are available. The corresponding
[server code](../server/librarian_server/) often has fairly extensive internal
documentation describing arguments and semantics.
