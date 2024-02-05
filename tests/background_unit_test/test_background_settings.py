"""
Tests our ability to serialize/deserialize the background settings.
"""

from librarian_background.settings import BackgroundSettings


def test_background_settings_full():
    BackgroundSettings.model_validate(
        {
            "check_integrity": [
                {
                    "task_name": "check",
                    "every": "01:00:00",
                    "age_in_days": 7,
                    "store_name": "test",
                }
            ],
            "create_local_clone": [
                {
                    "task_name": "clone",
                    "every": "22:23:02",
                    "age_in_days": 7,
                    "clone_from": "test",
                    "clone_to": "test",
                }
            ],
        }
    )


def test_background_settings_empty():
    BackgroundSettings()
