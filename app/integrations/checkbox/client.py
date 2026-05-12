from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx

from app.integrations.checkbox.config import CheckboxSettings


logger = logging.getLogger("checkbox.client")


class CheckboxClientError(RuntimeError):
    pass


class CheckboxClient:
    def __init__(self, settings: CheckboxSettings):
        self.settings = settings

    def _headers(self, token: str | None = None, *, include_license: bool = False) -> dict[str, str]:
        headers = {
            "accept": "application/json",
            "Content-Type": "application/json",
            "X-Client-Name": self.settings.client_name,
            "X-Client-Version": self.settings.client_version,
        }
        if self.settings.access_key:
            headers["X-Access-Key"] = self.settings.access_key
        if include_license:
            if not self.settings.license_key:
                raise CheckboxClientError("CHECKBOX_LICENSE_KEY is not configured")
            headers["X-License-Key"] = self.settings.license_key
        if token:
            headers["Authorization"] = f"Bearer {token}"
        return headers

    async def _request(
        self,
        method: str,
        path: str,
        *,
        token: str | None = None,
        include_license: bool = False,
        json_payload: dict[str, Any] | None = None,
        attempts: int = 3,
    ) -> dict[str, Any]:
        url = f"{self.settings.api_base_url}{path}"
        last_error: Exception | None = None
        async with httpx.AsyncClient(timeout=30.0) as client:
            for attempt in range(1, attempts + 1):
                try:
                    response = await client.request(
                        method,
                        url,
                        headers=self._headers(token, include_license=include_license),
                        json=json_payload,
                    )
                    if response.status_code == 204:
                        return {}
                    if response.status_code == 429 or response.status_code >= 500:
                        last_error = CheckboxClientError(
                            f"Checkbox {method} {path} status={response.status_code}: {response.text[:500]}"
                        )
                    else:
                        if not (200 <= response.status_code < 300):
                            raise CheckboxClientError(
                                f"Checkbox {method} {path} status={response.status_code}: {response.text[:500]}"
                            )
                        if not response.text:
                            return {}
                        return response.json()
                except httpx.RequestError as exc:
                    last_error = exc

                if attempt < attempts:
                    await asyncio.sleep(0.5 * attempt)

        raise CheckboxClientError(f"Checkbox {method} {path} failed after {attempts} attempts: {last_error}")

    async def signin(self) -> str:
        if self.settings.cashier_pin:
            response = await self._request(
                "POST",
                "/api/v1/cashier/signinPinCode",
                include_license=True,
                json_payload={"pin_code": self.settings.cashier_pin},
            )
        elif self.settings.cashier_login and self.settings.cashier_password:
            response = await self._request(
                "POST",
                "/api/v1/cashier/signin",
                json_payload={
                    "login": self.settings.cashier_login,
                    "password": self.settings.cashier_password,
                },
            )
        else:
            raise CheckboxClientError("CHECKBOX_CASHIER_PIN or CHECKBOX_CASHIER_LOGIN/PASSWORD is required")

        token = response.get("access_token")
        if not token:
            raise CheckboxClientError("Checkbox signin response has no access_token")
        return str(token)

    async def open_shift(self, token: str) -> dict[str, Any]:
        return await self._request(
            "POST",
            "/api/v1/shifts",
            token=token,
            include_license=True,
            json_payload={},
        )

    async def close_shift(self, token: str) -> dict[str, Any]:
        return await self._request(
            "POST",
            "/api/v1/shifts/close",
            token=token,
            include_license=True,
            json_payload={},
        )

    async def get_shift(self, token: str, shift_id: str) -> dict[str, Any]:
        return await self._request("GET", f"/api/v1/shifts/{shift_id}", token=token, include_license=True)

    async def create_sell_receipt(self, token: str, payload: dict[str, Any]) -> dict[str, Any]:
        return await self._request(
            "POST",
            "/api/v1/receipts/sell",
            token=token,
            include_license=True,
            json_payload=payload,
        )

    async def get_receipt(self, token: str, receipt_id: str) -> dict[str, Any]:
        return await self._request("GET", f"/api/v1/receipts/{receipt_id}", token=token, include_license=True)

    async def wait_receipt_done(self, token: str, receipt_id: str) -> dict[str, Any]:
        deadline = asyncio.get_running_loop().time() + self.settings.receipt_poll_timeout_sec
        last_response: dict[str, Any] = {}
        while True:
            last_response = await self.get_receipt(token, receipt_id)
            status = str(last_response.get("status") or "").upper()
            tx = last_response.get("transaction") if isinstance(last_response.get("transaction"), dict) else {}
            tx_status = str(tx.get("status") or "").upper()
            if status in {"DONE", "CLOSED"} or tx_status == "DONE" or last_response.get("fiscal_code"):
                return last_response
            if status in {"ERROR", "FAILED"} or tx_status in {"ERROR", "FAILED"}:
                raise CheckboxClientError(f"Checkbox receipt failed: receipt_id={receipt_id} response={last_response}")
            if asyncio.get_running_loop().time() >= deadline:
                return last_response
            await asyncio.sleep(self.settings.receipt_poll_interval_sec)
