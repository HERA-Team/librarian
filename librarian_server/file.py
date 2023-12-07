# -*- mode: python; coding: utf-8 -*-
# Copyright 2016-2017 the HERA Collaboration
# Licensed under the BSD License.

"Files."



__all__ = str('''
File
FileInstance
FileEvent
''').split()

import sys
import datetime
import json
import os.path
import re
from flask import flash, redirect, render_template, url_for

from . import app, db, logger
from .webutil import ServerError, json_api, login_required, optional_arg, required_arg
from .observation import Observation
# from .store import Store
from .orm.storemetadata import StoreMetadata

from .deletion import DeletionPolicy


def infer_file_obsid(parent_dirs, name, info):
    """Infer the obsid associated with a file based on the limited information we
    have about it. Raises an exception if this cannot be done *with
    certainty*.

    The "hera" mode does this by looking for existing files whose names start
    with the same "zen.JD" prefix.

    The "so" mode does this by looking for existing files whose names start
    with the same "book_id" prefix.

    The "none" mode refuses to do this.

    There is also a secret "_testing" mode.

    """
    mode = app.config.get('obsid_inference_mode', 'none')

    if mode == 'none':
        raise ServerError('refusing to try to infer the obsid of candidate new file \"%s\"', name)

    if mode == 'hera':
        bits = name.split('.')
        if len(bits) < 4:
            raise ServerError(
                'need to infer obsid of HERA file \"%s\", but its name looks weird', name)

        prefix = '.'.join(bits[:3])
        obsids = list(db.session.query(File.obsid)
                      .filter(File.name.like(prefix + '.%'))
                      .group_by(File.obsid))

        if len(obsids) != 1:
            raise ServerError('need to infer obsid of HERA file \"%s\", but got %d candidate '
                              'obsids from similarly-named files', name, len(obsids))

        return obsids[0]

    if mode == "so":
        bits = name.split("_")
        if len(bits) < 2:
            raise ServerError(
                "need to infer obsid of SO file \"%s\", but its name looks weird", name
            )

        prefix = "_".join(bits[:2])
        obsids = list(
            db.session.query(File.obsid)
            .filter(File.name.like(prefix + "_%"))
            .group_by(File.obsid)
        )

        if len(obsids) != 1:
            raise ServerError(
                "need to infer obsid of SO file \"%s\", but got %d candidate obsids from "
                "similarly-named files", name, len(obsids)
            )

        return obsids[0]

    if mode == '_testing':
        bits = name.split('.')
        if len(bits) < 4:
            raise ServerError(
                'need to infer obsid of _testing file \"%s\", but its name looks weird', name)

        jd = float(bits[1] + '.' + bits[2])
        from astropy.time import Time
        from math import floor
        return int(floor(Time(jd, format='jd', scale='utc').gps))

    raise ServerError('configuration problem: unknown "obsid_inference_mode" setting %r', mode)

class File:
    pass


class FileInstance:
    pass

class FileEvent:
    pass

# RPC endpoints

@app.route('/api/create_file_event', methods=['GET', 'POST'])
@json_api
def create_file_event(args, sourcename=None):
    """Create a FileEvent record for a File.

    We enforce basically no structure on the event data.

    """
    file_name = required_arg(args, str, 'file_name')
    type = required_arg(args, str, 'type')
    payload = required_arg(args, dict, 'payload')

    file = File.query.get(file_name)
    if file is None:
        raise ServerError('no known file "%s"', file_name)

    event = file.make_generic_event(type, **payload)
    db.session.add(event)

    try:
        db.session.commit()
    except SQLAlchemyError:
        db.session.rollback()
        app.log_exception(sys.exc_info())
        raise ServerError('failed to add event to database -- see server logs for details')

    return {}


@app.route('/api/locate_file_instance', methods=['GET', 'POST'])
@json_api
def locate_file_instance(args, sourcename=None):
    """Tell the caller where to find an instance of the named file.

    """
    file_name = required_arg(args, str, 'file_name')

    file = File.query.get(file_name)
    if file is None:
        raise ServerError('no known file "%s"', file_name)

    for inst in file.instances:
        return {
            'full_path_on_store': inst.full_path_on_store(),
            'store_name': inst.store_name,
            'store_path': inst.store_path,
            'store_ssh_host': inst.store_object.ssh_host,
        }

    raise ServerError('no instances of file "%s" on this librarian', file_name)


@app.route('/api/set_one_file_deletion_policy', methods=['GET', 'POST'])
@json_api
def set_one_file_deletion_policy(args, sourcename=None):
    """Set the deletion policy of one instance of a file.

    The "one instance" restriction is just a bit of a sanity-check to throw up
    barriers against deleting all instances of a file if more than one
    instance actually exists.

    If the optional 'restrict_to_store' argument is supplied, only instances
    on the specified store will be modified. This is useful when clearing out
    a store for deactivation (see also the "offload" functionality). Note that
    the "one instance" limit still applies.

    """
    file_name = required_arg(args, str, 'file_name')
    deletion_policy = required_arg(args, str, 'deletion_policy')
    restrict_to_store = optional_arg(args, str, 'restrict_to_store')
    if restrict_to_store is not None:
        from .orm.storemetadata import StoreMetadata
        restrict_to_store = StoreMetadata.from_name(restrict_to_store)  # ServerError if lookup fails

    file = File.query.get(file_name)
    if file is None:
        raise ServerError('no known file "%s"', file_name)

    deletion_policy = DeletionPolicy.parse_safe(deletion_policy)

    for inst in file.instances:
        # We could do this filter in SQL but it's easier to just do it this way;
        # you can't call filter() on `file.instances`.
        if restrict_to_store is not None and inst.store != restrict_to_store.id:
            continue

        inst.deletion_policy = deletion_policy
        break  # just one!
    else:
        raise ServerError('no instances of file "%s" on this librarian', file_name)

    db.session.add(file.make_generic_event('instance_deletion_policy_changed',
                                           store_name=inst.store_object.name,
                                           parent_dirs=inst.parent_dirs,
                                           new_policy=deletion_policy))

    try:
        db.session.commit()
    except SQLAlchemyError:
        db.session.rollback()
        app.log_exception(sys.exc_info())
        raise ServerError('failed to commit changes to the database')

    return {}


@app.route('/api/delete_file_instances', methods=['GET', 'POST'])
@json_api
def delete_file_instances(args, sourcename=None):
    """DANGER ZONE! Delete instances of the named file on all stores!

    See File.delete_instances for a description of the safety interlocks.

    """
    file_name = required_arg(args, str, 'file_name')
    mode = optional_arg(args, str, 'mode', 'standard')
    restrict_to_store = optional_arg(args, str, 'restrict_to_store')
    if restrict_to_store is not None:
        from .orm.storemetadata import StoreMetadata
        restrict_to_store = StoreMetadata.from_name(restrict_to_store)  # ServerError if lookup fails

    file = File.query.get(file_name)
    if file is None:
        raise ServerError('no known file "%s"', file_name)

    return file.delete_instances(mode=mode, restrict_to_store=restrict_to_store)


@app.route('/api/delete_file_instances_matching_query', methods=['GET', 'POST'])
@json_api
def delete_file_instances_matching_query(args, sourcename=None):
    """DANGER ZONE! Delete instances of lots of files on the store!

    See File.delete_instances for a description of the safety interlocks.

    """
    query = required_arg(args, str, 'query')
    mode = optional_arg(args, str, 'mode', 'standard')
    restrict_to_store = optional_arg(args, str, 'restrict_to_store')
    if restrict_to_store is not None:
        from .orm.storemetadata import StoreMetadata
        restrict_to_store = StoreMetadat.from_name(restrict_to_store)  # ServerError if lookup fails

    from .search import compile_search
    query = compile_search(query, query_type='files')
    stats = {}

    for file in query:
        stats[file.name] = file.delete_instances(mode=mode, restrict_to_store=restrict_to_store)

    return {
        'stats': stats,
    }


# Web user interface

@app.route('/files/<string:name>')
@login_required
def specific_file(name):
    file = File.query.get(name)
    if file is None:
        flash('No such file "%s" known' % name)
        return redirect(url_for('index'))

    instances = list(FileInstance.query.filter(FileInstance.name == name))
    events = sorted(file.events, key=lambda e: e.time, reverse=True)

    return render_template(
        'file-individual.html',
        title='%s File %s' % (file.type, file.name),
        file=file,
        instances=instances,
        events=events,
    )
