"""The Subscription API is the platform's contract (docs/api/openapi.json). build.py generates its client from that
file: hl7_subscriptions_client.api (one module per operation) and .models (one class per request and response). This
module only wires the client up: the API's URL, the caller's token on every call (in a Databricks notebook, exchanged
for one the API's App accepts), and reading the error body of a refused call. (The Java example's api/Api.java does the
same; it doesn't run in notebooks, so it has no exchange.)
"""

import json
import threading
import time
from urllib.parse import urlparse

import httpx2
from hl7_subscriptions_client import Client
from hl7_subscriptions_client.models import Declared, Error, Item, Refusal, RefusedResult

EXCHANGE_MARGIN = 60  # seconds before an exchanged token expires


class _Bearer(httpx2.Auth):
    """A fresh token (the caller's OAuth token; the SDK refreshes it) on every request."""

    def __init__(self, token):
        self._token = token

    def auth_flow(self, request):
        request.headers["Authorization"] = f"Bearer {self._token()}"
        yield request


class Exchanged:
    """A Databricks notebook's own token can't call a Databricks App. The workspace's OAuth endpoint exchanges it for
    one whose audience is the App (Databricks docs: connect to an API Databricks app from a notebook), good for that App
    only. Called for each request: cached, and exchanged again shortly before it expires."""

    def __init__(self, host: str, subject_token, audience: str, transport=None, clock=time.time):
        self._url, self._subject, self._audience = f"{host.rstrip('/')}/oidc/v1/token", subject_token, audience
        self._http = httpx2.Client(timeout=30.0, transport=transport)
        self._clock, self._current, self._lock = clock, None, threading.Lock()

    def __call__(self) -> str:
        with self._lock:
            if self._current is None or self._clock() >= self._current[1] - EXCHANGE_MARGIN:
                self._current = self._exchange()
            return self._current[0]

    def _exchange(self) -> tuple[str, float]:
        r = self._http.post(self._url, data={
            "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
            "subject_token": self._subject(),
            "subject_token_type": "urn:databricks:params:oauth:token-type:personal-access-token",
            "requested_token_type": "urn:ietf:params:oauth:token-type:access_token",
            "scope": "all-apis",
            "audience": self._audience,
        })  # fmt: skip
        try:
            body = r.json()
        except ValueError:
            body = {}
        if r.status_code != 200:
            why = body.get("error_description") or r.text
            raise RuntimeError(f"the workspace didn't exchange the notebook's token for the Subscription API's App: "
                               f"{r.status_code} {body.get('error')}: {why}")  # fmt: skip
        if not body.get("access_token"):
            raise RuntimeError("the workspace's token exchange returned no access_token")
        return body["access_token"], self._clock() + float(body.get("expires_in") or 0)


def connect(url: str, bearer) -> Client:
    """A client for the API at `url`, sending `bearer()` (the caller's OAuth token) on every call."""
    if urlparse(url).scheme != "https":
        raise ValueError(f"api.url must be https: {url}")
    # A status call waits at most 60 s on the server.
    return Client(base_url=url.rstrip("/"), timeout=httpx2.Timeout(90.0), httpx_args={"auth": _Bearer(bearer)})


class ApiError(Exception):
    """A call that didn't return 200: its status and the contract's error body ({code, message}, or 422's refusals)."""

    def __init__(self, status: int, parsed, content: bytes):
        self.status, self.parsed, self.content = status, parsed, content
        super().__init__(f"{self.code}: {self.message}")

    def _body(self) -> dict:
        try:
            return json.loads(self.content)
        except ValueError:
            return {}

    @property
    def code(self) -> str:
        if isinstance(self.parsed, Error):
            return self.parsed.code
        return self._body().get("code") or f"HTTP_{self.status}"

    @property
    def message(self) -> str:
        if isinstance(self.parsed, Error):
            return self.parsed.message
        return self._body().get("message") or self.content.decode(errors="replace")

    @property
    def refusals(self) -> list[Refusal]:
        """A 422's refusals: each rule's Databricks error (or ours). Empty for any other failure."""
        if self.status != 422:
            return []
        if isinstance(self.parsed, RefusedResult) and self.parsed.refused:
            return list(self.parsed.refused)
        return [Refusal(code=self.code, message=self.message)]


def call(operation, client: Client, **kwargs):
    """Run a generated operation (e.g. `call(declare, client, body=...)`): its 200 result, else ApiError."""
    r = operation.sync_detailed(client=client, **kwargs)
    if r.status_code == 200:
        return r.parsed
    raise ApiError(int(r.status_code), r.parsed, r.content)


def declared(subscriptions: dict[str, str]) -> Declared:
    """The request body for a declare or validate call: name -> SQL, in order."""
    return Declared(subscriptions=[Item(name=name, sql=sql) for name, sql in subscriptions.items()])
