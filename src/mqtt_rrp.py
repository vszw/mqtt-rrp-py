import json, asyncio, uuid
from typing import Any, List
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
    
    async def request(self, topic: str, data: Any, return_full: bool = False):
        request_id = str(uuid.uuid4())
        payload = self.__serialize(data, request_id=request_id)
        
        self.client.publish(topic, payload)
        
        # wait for response from event emitter or timeout
        future = asyncio.Future()
        self.once(request_id, lambda payload: future.set_result(payload))
        
        try:
            payload = await asyncio.wait_for(future, self.request_timeout)
            payload = self.__deserialize(payload.data)
        except asyncio.TimeoutError:
            raise TimeoutError(f'Request timed out. Topic: {topic}, request_id: {request_id}')
        
        if return_full:
            return payload
        else:
            return payload.data