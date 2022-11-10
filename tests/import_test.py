import insight
import os
import sys
import unittest
import json
from unittest.mock import patch, MagicMock

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

# Mock calls to atlassian API so we can test without side-effects
insight.insightConnect.getWorkspaceId = MagicMock(
    return_value="fake-workspace-id")


class TestImport(unittest.TestCase):

    def test_escape(self):
        test_cases = [
            ('a "b" c', 'a \\"b\\" c'),
            ('a \\"b\\" c', 'a \\"b\\" c'),
            ('"a \\"b\\" c\\', '\\"a \\"b\\" c\\'),
            ('\\a \\"b\\" c"', '\\a \\"b\\" c\\"'),
        ]
        for arg, expected in test_cases:
            got = insight.escape(arg)
            if got != expected:
                self.fail(f"""expected '{expected}', got: '{got}' """)

    # prevent side-effects (calls to the atlassian API)
    @patch.multiple(insight.insightConnect,
                    insightGet=MagicMock(side_effect=NotImplemented),
                    getAttributeByName=MagicMock(return_value={'id': '456', 'type': 0, 'defaultType': {'id': 10}}))
    def test_quoted_string_import_conversion(self, **_):
        test_cases = [
            ({'SomeAttribute': 'some value with "quoted text" inside"'},
             'some value with "quoted text" inside"'),

            ({'SomeAttribute': 'some value with escaped \\"quoted text\\" inside'},
             'some value with escaped "quoted text" inside'),

            ({'SomeAttribute': ' \\"escaped\\" mixed with \\"quoted"'},
             ' "escaped" mixed with "quoted"'),
        ]

        client = insight.insightConnect("jiraUrl", "username", "apiToken")

        for arg, expected_str in test_cases:
            json_str = client.constructObjectPayload(arg, "123")

            # ensure the produced string is valid json. Fails the test if not.
            actual = json.loads(json_str)

            # ensure we obtain the expected object structure and content as well
            expected_dict = {
                'objectTypeId': '123',
                'attributes': [{
                    'objectTypeAttributeId': '456',
                    'objectAttributeValues': [{
                        'value': expected_str
                    }]
                }]
            }

            self.assertEqual(expected_dict, actual)
