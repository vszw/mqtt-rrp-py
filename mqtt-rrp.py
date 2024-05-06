from typing import Any, List
from event_emitter import EventEmitter
import paho.mqtt.client as mqtt 


class TPayload:
    ts: int
    data: Any
    from_topic: str
    request_id: str | None
    callback_id: str | None


class MQTTRequestResponseProtocol(EventEmitter):
    client: mqtt.Client
    identifier: str
    topics: List[tuple[str, int]] = []
    initialized: bool = False
    request_timeout: int = 3
    
    def __init__(self, client: mqtt.Client, identifier: str):
        super().__init__()
        self.client = client
        self.identifier = identifier
        self.topics = []
    
    def initialize(self, topics: List[tuple[str, int]]):
        self.topics = topics
        self.client.on_connect = self.__on_connect
        self.client.on_message = self.__on_message
    
    def connect(self, host: str, port: int, user: str | None = None, password: str | None = None, keepalive: int = 60):
        self.client.username_pw_set(user, password)
        self.client.connect(host, port, keepalive)
        self.client.loop_start()
    
    def __on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self.initialized = True
            self.emit('connected')
            self.client.subscribe(self.topics)
        else:
            self.emit('error', rc)
    
    def __on_message(self, client, userdata, msg):
        pass