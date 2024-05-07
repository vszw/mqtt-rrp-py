import json, asyncio, uuid
from typing import Any, List, Awaitable
from event_emitter import EventEmitter
from paho.mqtt.client import MQTTMessage, Client


class TPayload:
    ts: int
    data: Any
    from_topic: str
    request_id: str | None
    callback_id: str | None


class MQTTRequestResponseProtocol(EventEmitter):
    client: Client
    identifier: str
    topics: List[tuple[str, int]] = []
    initialized: bool = False
    request_timeout: int = 3
    
    def __init__(self, client: Client, identifier: str):
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
    
    def __deserialize(self, payload: bytes) -> TPayload:
        return json.loads(payload)
    
    def __serialize(self, data: Any, request_id: str | None = None, callback_id: str | None = None) -> bytes:
        return json.dumps(TPayload(ts=0, data=data, from_topic=self.identifier, request_id=request_id, callback_id=callback_id)).encode()
    
    def __on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self.initialized = True
            self.emit('connected')
            self.client.subscribe(self.topics)
        else:
            self.emit('error', rc)
    
    def __on_message(self, client, userdata, msg: MQTTMessage):
        try:
            payload = self.__deserialize(msg.payload)
        except:
            self.emit('error', f'Invalid payload. Topic: {msg.topic}')
        
        if payload.callback_id is not None and msg.topic == self.identifier:
            self.emit(payload.callback_id, payload)
            return
        
        self.emit('message', payload)
    
    def send(self, topic: str, data: Any, callback_id: str | None = None):
        payload = self.__serialize(data, callback_id=callback_id)
        self.client.publish(topic, payload)
    
    def request(self, topic: str, data: Any, return_full: bool = False) -> Awaitable[TPayload | Any]:
        request_id = str(uuid.uuid4())
        payload = self.__serialize(data, request_id=request_id)
        
        self.client.publish(topic, payload)
        future: asyncio.Future[TPayload] = asyncio.Future()
        
        self.listeners[request_id].append(lambda payload: future.set_result(payload))
        return future if return_full else future.then(lambda p: p.data)
