from django.core.exceptions import ImproperlyConfigured
from django.test import SimpleTestCase

from config.service_credentials import service_credentials


class ServiceCredentialsTests(SimpleTestCase):
    def test_legacy_credentials_remain_the_default(self):
        self.assertEqual(
            service_credentials({}, "APP_POSTGRES", "legacy", "legacy-password"),
            ("legacy", "legacy-password"),
        )

    def test_distinct_service_credentials_override_both_values(self):
        for prefix in ("APP_POSTGRES", "APP_CLICKHOUSE"):
            with self.subTest(prefix=prefix):
                self.assertEqual(
                    service_credentials(
                        {f"{prefix}_USER": "service", f"{prefix}_PASSWORD": "service-password"},
                        prefix, "legacy", "legacy-password",
                    ),
                    ("service", "service-password"),
                )

    def test_incomplete_or_empty_credentials_fail_closed(self):
        for prefix in ("APP_POSTGRES", "APP_CLICKHOUSE"):
            for entries in (
                {f"{prefix}_USER": "service"},
                {f"{prefix}_PASSWORD": "secret"},
                {f"{prefix}_USER": "", f"{prefix}_PASSWORD": "secret"},
                {f"{prefix}_USER": "  ", f"{prefix}_PASSWORD": "secret"},
                {f"{prefix}_USER": "service", f"{prefix}_PASSWORD": ""},
            ):
                with self.subTest(prefix=prefix, entries=list(entries)):
                    with self.assertRaisesRegex(ImproperlyConfigured, "must both be set"):
                        service_credentials(entries, prefix, "legacy", "legacy-password")
