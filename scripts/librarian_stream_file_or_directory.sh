#! /bin/sh
# -*- mode: python; coding: utf-8 -*-
# Copyright 2016 the HERA Team.
# Licensed under the BSD License.

if [ x"$1" = x ] ; then
    echo >&2 "$0: error: no input path specified"
    exit 1
fi

path="$1"

if [ -d "$path" ] ; then
    cd $(dirname "$path")
    exec tar c $(basename "$path")
else
    exec cat "$path"
fi
