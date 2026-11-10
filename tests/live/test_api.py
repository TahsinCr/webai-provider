import os
import unittest

from tests.live.support.case import LiveProviderCase
from tests.live.test_chats import ChatApiTestsMixin
from tests.live.test_messages import MessageApiTestsMixin
from tests.live.test_models import ModelApiTestsMixin
from tests.live.test_projects import ProjectApiTestsMixin
from tests.live.test_sessions import SessionApiTestsMixin


@unittest.skipUnless(
    os.environ.get("WEBAI_LIVE_TESTS") == "1",
    "Set WEBAI_LIVE_TESTS=1 to opt in to real provider requests",
)
# Scenario mixins share one live session; this is the single discovered TestCase.
class TestLiveProviderAPI(
    SessionApiTestsMixin,
    ModelApiTestsMixin,
    ChatApiTestsMixin,
    ProjectApiTestsMixin,
    MessageApiTestsMixin,
    LiveProviderCase,
    unittest.TestCase,
):
    """Runs the selected account through the shared live API session."""
