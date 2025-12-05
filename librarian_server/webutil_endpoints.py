# Copyright 2025 the HERA Collaboration
# Licensed under the BSD License.

"""Login/logout enpoints for the webutils."""


__all__ = str(
    """
login
logout
"""
).split()

from flask import flash, redirect, render_template, request, session, url_for

from . import app
from .webutil import _check_authentication, AuthFailedError



@app.route("/login", methods=["GET", "POST"])
def login():
    if len(request.form):
        reqdata = request.form  # POST
    else:
        reqdata = request.args  # GET

    nxt = reqdata.get("next")
    if nxt is None:
        nxt = url_for("index")

    if request.method == "GET":
        return render_template("login.html", next=nxt)

    # This is a POST request -- user is actually trying to log in.

    try:
        sourcename = _check_authentication(request.form.get("auth"))
    except AuthFailedError:
        flash("Login failed.")
        return render_template("login.html", next=nxt)

    session["sourcename"] = sourcename
    return redirect(nxt)


@app.route("/logout")
def logout():
    session.pop("sourcename", None)
    return redirect(url_for("index"))
