"""Unit tests for aioads.functions.ads_symbol_upload_info."""

import unittest
from unittest.mock import AsyncMock

from aioads.functions.ads_symbol_upload_info import (
    AdsSymbolUploadInfo2,
    SymbolUploadInfo2Response,
)
from tests.builders import (
    make_ams_address,
    make_ams_header,
    make_read_payload,
    make_stream,
    make_transport,
)


class TestSymbolUploadInfo2Response(unittest.TestCase):

    def setUp(self) -> None:
        self.response = SymbolUploadInfo2Response(
            symbol_cnt=10,
            symbol_size=2048,
            symbols_max_dynamic_cnt=100,
            symbol_used_dynamic_cnt=5,
            datatype_cnt=40,
            datatype_size=4096,
        )

    def test_serialize_produces_six_uint32_fields(self) -> None:
        # Act
        result = self.response.serialize()

        # Assert
        self.assertEqual(len(result), 24)

    def test_serialize_then_deserialize_round_trips(self) -> None:
        # Act
        restored = SymbolUploadInfo2Response.deserialize(
            make_stream(self.response.serialize())
        )

        # Assert
        self.assertEqual(restored, self.response)


class TestAdsSymbolUploadInfo2(unittest.IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        self.transport = make_transport()
        self.function = AdsSymbolUploadInfo2(
            transport=self.transport,
            ams_address=make_ams_address(),
        )

    def test_serialize_returns_empty_payload(self) -> None:
        # Act / Assert
        self.assertEqual(self.function.serialize(), b"")

    async def test_execute_returns_deserialized_upload_info(self) -> None:
        # Arrange
        info = SymbolUploadInfo2Response(1, 2048, 3, 4, 40, 4096)
        payload = make_read_payload(info.serialize())
        self.transport.request = AsyncMock(
            return_value=(make_ams_header(), make_stream(payload))
        )

        # Act
        result = await self.function.execute()

        # Assert
        self.assertEqual(result, info)


if __name__ == "__main__":
    unittest.main()
