"""Pub/Sub via google-cloud-pubsub (synchronous pull)."""

from __future__ import annotations

from typing import Any

from google.api_core import exceptions
from google.cloud import pubsub_v1

from cloud.base import NotFound, PubSub, ReceivedMessage


class GcpPubSub(PubSub):
    def __init__(self, project: str):
        self.project = project
        self.publisher = pubsub_v1.PublisherClient()
        self.subscriber = pubsub_v1.SubscriberClient()

    def _topic(self, topic: str) -> str:
        return self.publisher.topic_path(self.project, topic)

    def _sub(self, subscription: str) -> str:
        return self.subscriber.subscription_path(self.project, subscription)

    def ensure_topic(self, topic: str) -> None:
        try:
            self.publisher.create_topic(request={"name": self._topic(topic)})
        except exceptions.AlreadyExists:
            pass

    def ensure_subscription(self, subscription: str, topic: str, ack_deadline_s: int = 60) -> None:
        try:
            self.subscriber.create_subscription(request={
                "name": self._sub(subscription),
                "topic": self._topic(topic),
                "ack_deadline_seconds": min(ack_deadline_s, 600),  # GCP max is 600s
            })
        except exceptions.AlreadyExists:
            pass
        except exceptions.NotFound as exc:
            raise NotFound(f"Topic {topic!r} does not exist") from exc

    def publish(self, topic: str, data: bytes, **attributes: str) -> str:
        future = self.publisher.publish(self._topic(topic), data, **{k: str(v) for k, v in attributes.items()})
        try:
            return future.result(timeout=60)
        except exceptions.NotFound as exc:
            raise NotFound(f"Topic {topic!r} does not exist") from exc

    def pull(self, subscription: str, max_messages: int = 10) -> list[ReceivedMessage]:
        try:
            resp = self.subscriber.pull(
                request={"subscription": self._sub(subscription), "max_messages": max_messages}, timeout=15
            )
        except exceptions.DeadlineExceeded:
            return []
        except exceptions.NotFound as exc:
            raise NotFound(f"Subscription {subscription!r} does not exist") from exc
        return [
            ReceivedMessage(
                ack_id=rm.ack_id,
                message_id=rm.message.message_id,
                data=rm.message.data,
                attributes=dict(rm.message.attributes),
                publish_time=rm.message.publish_time,
                delivery_attempt=rm.delivery_attempt or None,
            )
            for rm in resp.received_messages
        ]

    def ack(self, subscription: str, ack_ids: list[str]) -> None:
        if ack_ids:
            self.subscriber.acknowledge(request={"subscription": self._sub(subscription), "ack_ids": ack_ids})

    def nack(self, subscription: str, ack_ids: list[str]) -> None:
        if ack_ids:
            self.subscriber.modify_ack_deadline(request={
                "subscription": self._sub(subscription), "ack_ids": ack_ids, "ack_deadline_seconds": 0,
            })

    def list_topics(self) -> list[str]:
        topics = self.publisher.list_topics(request={"project": f"projects/{self.project}"})
        return sorted(t.name.rsplit("/", 1)[-1] for t in topics)

    def list_subscriptions(self) -> list[dict[str, Any]]:
        subs = self.subscriber.list_subscriptions(request={"project": f"projects/{self.project}"})
        return sorted(
            ({"name": s.name.rsplit("/", 1)[-1], "topic": s.topic.rsplit("/", 1)[-1],
              "ack_deadline_s": s.ack_deadline_seconds} for s in subs),
            key=lambda s: s["name"],
        )
