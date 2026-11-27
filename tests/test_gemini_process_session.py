from __future__ import annotations

import base64
import json
import unittest
from types import SimpleNamespace

from webaiprovider.providers.gemini.proto import ProtoView, encode_message
from webaiprovider.providers.gemini.webchannel import (
    ProcessSessionFrameProcessor,
    WebChannelFrameDecoder,
    build_process_session_headers,
    build_process_session_turn_data,
    extract_handshake_sid,
    iter_protobuf_packets,
    sapisidhash_header,
)
from webaiprovider.resources.message.protocol import ProviderRequest
from webaiprovider.resources.message.types import MessageAction


class GeminiProcessSessionTests(unittest.TestCase):
    def test_proto_roundtrip_and_path_access(self) -> None:
        raw = encode_message(
            {
                1: {
                    8: {
                        5: {
                            1: {
                                1: "hello",
                                46: 1,
                                55: [1, 7],
                                80: 3,
                                81: 1,
                                97: 1,
                            }
                        }
                    }
                }
            }
        )
        view = ProtoView(raw)
        inner = view.at(1, 8, 5, 1)
        assert inner is not None
        self.assertEqual(inner.get_str(1), "hello")
        self.assertEqual(inner.get_int(46), 1)
        self.assertEqual(inner.all_raw(55), [1, 7])
        self.assertEqual(inner.get_int(80), 3)
        self.assertEqual(inner.get_int(97), 1)

    def test_sapisidhash_and_temporary_headers(self) -> None:
        auth = sapisidhash_header({"__Secure-1PAPISID": "secret-papisid"})
        assert auth is not None
        self.assertTrue(auth.startswith("SAPISIDHASH "))
        headers = build_process_session_headers(
            {"__Secure-1PAPISID": "secret-papisid"},
            temporary=True,
            gsessionid="gs123",
        )
        self.assertEqual(headers["X-HTTP-Session-Id"], "gs123")
        self.assertEqual(headers["x-goog-ext-525001261-bin"], base64.b64encode(b"\x01").decode())

    def test_webchannel_frame_decoder_and_processor(self) -> None:
        handshake_payload = json.dumps([[0, ["c", "SID-999", "", 8]]])
        handshake_wire = f"{len(handshake_payload)}\n{handshake_payload}".encode("utf-8")
        decoder = WebChannelFrameDecoder()
        frames = decoder.feed(handshake_wire, final=True)
        self.assertEqual(extract_handshake_sid(frames), "SID-999")

        proto_packet = encode_message(
            {
                2: {
                    7: {
                        1: {
                            2: {1: "r_101"},
                            4: {
                                1: "rc_101",
                                2: "Merhaba!",
                                4: {
                                    1: "rc_101",
                                    2: "c_101",
                                    11: "Sohbet",
                                    18: "r_101",
                                    26: "tok_abc",
                                },
                            },
                        }
                    }
                }
            }
        )
        batch_payload = json.dumps([[1, [base64.b64encode(proto_packet).decode("ascii")]]])
        batch_wire = f"{len(batch_payload)}\n{batch_payload}".encode("utf-8")
        batch_frames = WebChannelFrameDecoder().feed(batch_wire, final=True)
        packets = iter_protobuf_packets(batch_frames[0])
        self.assertEqual(len(packets), 1)

        proc = ProcessSessionFrameProcessor(ProviderRequest("hi"), [], None)
        events = list(proc.feed_packet(packets[0]))
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["cid"], "c_101")
        self.assertEqual(events[0]["text"], "Merhaba!")
        self.assertEqual(events[0]["metadata"][9], "tok_abc")
        proc.verify_complete()

    def test_attachments_and_retry_turn_token_encoding(self) -> None:
        model = SimpleNamespace(
            id="gemini-neon",
            efforts=(SimpleNamespace(id="standard"),),
            default_effort="standard",
            metadata={"model_id": "neon", "number": 3, "effort_values": {"standard": 1}},
        )
        session = SimpleNamespace(models=(model,), _cookies={"SAPISID": "abc"})
        request = ProviderRequest("retry this", conversation_id="c_1", action=MessageAction.RETRY)
        _, form = build_process_session_turn_data(
            session,
            request,
            ["c_1", "r_1", "rc_1", None, None, None, None, None, None, "tok_prev"],
            [[["/contrib_service/ttl_1d/123"], "doc.docx"]],
        )
        self.assertEqual(form["count"], "2")
        req0 = ProtoView(base64.b64decode(form["req0___data__"])).at(1, 8, 5, 1)
        req1 = ProtoView(base64.b64decode(form["req1___data__"])).at(2, 9, 3)
        assert req0 is not None and req1 is not None
        self.assertEqual(req0.str_at(3, 10), "tok_prev")
        self.assertEqual(req0.get_int(73), 5)
        self.assertEqual(req1.get_str(1), "/contrib_service/ttl_1d/123")
        self.assertEqual(req1.get_str(2), "doc.docx")


if __name__ == "__main__":
    unittest.main()
