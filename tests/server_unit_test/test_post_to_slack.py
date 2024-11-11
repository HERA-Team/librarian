"""
Tests the 'post to slack' functionality.
"""

import datetime
import inspect


def test_post_to_slack():
    # This is a manual-only test. You will need to have a valid install.
    # If you want to run this test, you will need to set the following
    # env vars: LIBRARIAN_SERVER_SLACK_WEBHOOK_ENABLE,
    # LIBRARIAN_SERVER_SLACK_WEBHOOK_URL
    try:
        from librarian_server.settings import server_settings
    except:
        return

    if not server_settings.slack_webhook_enable:
        return

    from librarian_server.logger import (
        ErrorCategory,
        ErrorSeverity,
        post_error_to_slack,
    )

    class MockError:
        def __init__(self, severity, category, message, id):
            self.message = message
            self.category = category
            self.severity = severity
            self.id = id
            self.raised_time = datetime.datetime.now(datetime.timezone.utc)
            self.caller = (
                inspect.stack()[1].filename
                + ":"
                + inspect.stack()[1].function
                + ":"
                + str(inspect.stack()[1].lineno)
            )

    post_error_to_slack(
        MockError(
            ErrorSeverity.CRITICAL,
            ErrorCategory.DATA_AVAILABILITY,
            "This is a test message, please ignore.",
            12345,
        )
    )
