"""Reading this app's own Event Hub with no Azure secret. Unity Catalog issues a short-lived Entra token from the app's
service credential (sub_<app>_eventhubs; ACCESS for this app's SP only, Data Receiver on this app's hub only), Kafka
gets it through the OAUTHBEARER callback, and the client follows Microsoft's Event Hubs guidance. (= the Java
platform/EventHubs.)
"""

import threading
import time

from databricks.sdk.service.catalog import GenerateTemporaryServiceCredentialAzureOptions

REFRESH_MARGIN = 300  # seconds before expiry

# learn.microsoft.com/azure/event-hubs/apache-kafka-configurations: Azure closes idle connections after 240 s.
_EVENT_HUBS = {"metadata.max.age.ms": 180_000, "connections.max.idle.ms": 180_000, "socket.keepalive.enable": True}
_QUIET = {"log_level": 3}  # library logs at errors only, so the output stays readable (the Java example: WARN)


class Tokens:
    """A token source, cached until close to expiry: (token, expires_at in epoch seconds)."""

    def __init__(self, fetch, clock=time.time):
        self._fetch, self._clock, self._current, self._lock = fetch, clock, None, threading.Lock()

    def get(self) -> tuple[str, float]:
        with self._lock:
            if self._current is None or self._clock() > self._current[1] - REFRESH_MARGIN:
                token = self._fetch()
                if not token or not token[0]:
                    raise RuntimeError("Unity Catalog returned no Event Hubs token")
                self._current = token
            return self._current


def mint(workspace, credential: str, scope: str) -> tuple[str, float]:
    """One token from Unity Catalog for `scope`; PERMISSION_DENIED unless this identity has ACCESS on `credential`. Both
    come from the Subscription API (get_connection)."""
    c = workspace.credentials.generate_temporary_service_credential(
        credential_name=credential, azure_options=GenerateTemporaryServiceCredentialAzureOptions(resources=[scope])
    )
    return c.azure_aad.aad_token, c.expiration_time / 1000


def tokens_for(workspace, credential: str, scope: str) -> Tokens:
    return Tokens(lambda: mint(workspace, credential, scope))


def _auth(bootstrap_servers: str, tokens: Tokens) -> dict:
    return {"bootstrap.servers": bootstrap_servers, "security.protocol": "SASL_SSL", "sasl.mechanism": "OAUTHBEARER",
            "oauth_cb": lambda _config: tokens.get(), **_EVENT_HUBS, **_QUIET}  # fmt: skip


def consumer_config(bootstrap_servers: str, group: str, tokens: Tokens) -> dict:
    """confluent-kafka Consumer settings: Entra OAUTHBEARER (no keys) plus Microsoft's Event Hubs client guidance."""
    return {**_auth(bootstrap_servers, tokens), "group.id": group, "enable.auto.commit": False,
            "auto.offset.reset": "latest", "session.timeout.ms": 30_000}  # fmt: skip


def producer_config(bootstrap_servers: str, tokens: Tokens) -> dict:
    """Producer settings (the E2E checks use one to prove the app can't write to its own hub)."""
    return {**_auth(bootstrap_servers, tokens), "request.timeout.ms": 20_000, "message.timeout.ms": 30_000}
