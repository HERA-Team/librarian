"""
A command-line script, for use in containers, to change the behaviour
of transfers to external librarians.
"""

import argparse as ap

parser = ap.ArgumentParser(
    description=(
        "Change the status of an external librarian, to enable or disable transfers."
    )
)

parser.add_argument(
    "--librarian",
    help="Name of the librarian to change the status of.",
    type=str,
    required=True,
)

parser.add_argument(
    "--enable",
    help="Enable the librarian.",
    action="store_true",
)

parser.add_argument(
    "--disable",
    help="Disable the librarian.",
    action="store_true",
)


def main():
    args = parser.parse_args()

    if args.enable and args.disable:
        raise ValueError("Cannot enable and disable at the same time.")

    if not args.enable and not args.disable:
        raise ValueError("Must enable or disable.")

    from librarian_server.database import get_session
    from librarian_server.orm import Librarian

    with get_session() as session:
        librarian = (
            session.query(Librarian).filter_by(name=args.librarian).one_or_none()
        )
        if librarian is None:
            raise ValueError(f"Librarian {args.librarian} does not exist.")

        if args.enable:
            librarian.transfers_enabled = True
        elif args.disable:
            librarian.transfers_enabled = False

        session.commit()
