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
        """
        Initializes a new instance of the class.

        Args:
            client (Client): The MQTT client to use.
            identifier (str): The identifier for the MQTT client.

        Returns:
            None
        """
        super().__init__()
        self.client = client
        self.identifier = identifier
        self.topics = []
    
    def initialize(self, topics: List[tuple[str, int]]):
        """
        Initializes the MQTTRequestResponseProtocol with the provided topics.

        Args:
            topics (List[tuple[str, int]]): The topics to subscribe to.

        Returns:
            None
        """
        self.topics = topics
        self.client.on_connect = self.__on_connect
        self.client.on_message = self.__on_message
    
    def connect(self, host: str, port: int, user: str | None = None, password: str | None = None, keepalive: int = 60):
        """
        Connects to an MQTT broker.

        Args:
            host (str): The hostname or IP address of the MQTT broker.
            port (int): The port number to connect to.
            user (str, optional): The username for authentication. Defaults to None.
            password (str, optional): The password for authentication. Defaults to None.
            keepalive (int, optional): The keepalive interval in seconds. Defaults to 60.

        Returns:
            None
        """
        self.client.username_pw_set(user, password)
        self.client.connect(host, port, keepalive)
        self.client.loop_start()
    
    def __deserialize(self, payload: bytes) -> TPayload:
        """
        Deserialize the given payload from bytes to TPayload.

        Args:
            payload (bytes): The payload to deserialize.

        Returns:
            TPayload: The deserialized payload.
        """
        return json.loads(payload)
    
    def __serialize(self, data: Any, request_id: str | None = None, callback_id: str | None = None) -> bytes:
        """
        Serialize the given data into bytes using TPayload with optional request_id and callback_id.

        Args:
            data (Any): The data to be serialized.
            request_id (str | None, optional): The request ID. Defaults to None.
            callback_id (str | None, optional): The callback ID. Defaults to None.

        Returns:
            bytes: The serialized data.
        """
        return json.dumps(TPayload(ts=0, data=data, from_topic=self.identifier, request_id=request_id, callback_id=callback_id)).encode()
    
    def __on_connect(self, client, userdata, flags, rc):
        """
        A callback function that is called when the client successfully connects to the MQTT broker.

        Args:
            client: The client instance for this callback.
            userdata: The private user data as set in Client() or userdata_set().
            flags: Response flags sent by the broker.
            rc: The connection result. 0 means success.

        Returns:
            None
        """
        if rc == 0:
            self.initialized = True
            self.emit('connected')
            self.client.subscribe(self.topics)
        else:
            self.emit('error', rc)
    
    def __on_message(self, client, userdata, msg: MQTTMessage):
        """
        A callback function that is called when a message is received by the MQTT client.

        Args:
            client (paho.mqtt.client.Client): The client instance for this callback.
            userdata: The private user data as set in Client() or userdata_set().
            msg (paho.mqtt.client.MQTTMessage): The received message.

        Returns:
            None
        """
        try:
            payload = self.__deserialize(msg.payload)
        except:
            self.emit('error', f'Invalid payload. Topic: {msg.topic}')
        
        if payload.callback_id is not None and msg.topic == self.identifier:
            self.emit(payload.callback_id, payload)
            return
        
        self.emit('message', payload)
    
    def send(self, topic: str, data: Any, callback_id: str | None = None):
        """
        Sends a message to the specified topic using the MQTT client.

        Args:
            topic (str): The topic to which the message will be published.
            data (Any): The data to be sent as the payload of the message.
            callback_id (str | None, optional): The callback ID associated with the message. Defaults to None.

        Returns:
            None
        """
        payload = self.__serialize(data, callback_id=callback_id)
        self.client.publish(topic, payload)
    
    def request(self, topic: str, data: Any, return_full: bool = False) -> Awaitable[TPayload | Any]:
        """
        Sends a request to the specified MQTT topic with the given data.

        Args:
            topic (str): The MQTT topic to which the request will be published.
            data (Any): The data to be sent as the payload of the request.
            return_full (bool, optional): Whether to return the full TPayload or just the data. Defaults to False.

        Returns:
            Awaitable[TPayload | Any]: A future that will resolve to the response payload. If return_full is True, the
            future will resolve to a TPayload object, otherwise it will resolve to the data field of the TPayload object.
        """
        request_id = str(uuid.uuid4())
        payload = self.__serialize(data, request_id=request_id)
        
        self.client.publish(topic, payload)
        future: asyncio.Future[TPayload] = asyncio.Future()
        
        def future_wrapper(payload: TPayload):
            future.set_result(payload)
            self.listeners[request_id].remove(future_wrapper)
        
        self.listeners[request_id].append(lambda payload: future_wrapper(payload))
        return future if return_full else future.then(lambda p: p.data)
