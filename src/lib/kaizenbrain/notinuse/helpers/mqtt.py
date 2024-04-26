import paho.mqtt.client as mqtt


class MQTTHelper:
    mqtt_client = None
    mqtt_user = None
    mqtt_password = None
    mqtt_host = None
    mqtt_client_id = None
    topic = ""

    def __init__(
        self, config, user, password, host, topic, client_id="mqtt_helper"
    ) -> None:
        self.config = config
        self.mqtt_user = user
        self.mqtt_password = password
        self.mqtt_host = host
        self.mqtt_client_id = client_id
        self.topic = topic

    def connect(self):
        def on_connect(client, userdata, flags, reason_code, properties=None):
            if reason_code == 0:
                print("Connected to MQTT Broker!")
            else:
                print("Failed to connect, return code %d\n", reason_code)

        self.mqtt_client = mqtt.Client(
            client_id=self.mqtt_client_id,
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        )
        self.mqtt_client.on_connect = on_connect
        self.mqtt_client.username_pw_set(self.mqtt_user, self.mqtt_password)
        self.mqtt_client.connect(self.mqtt_host, 1883, 60)

    def publish(self, message):
        self.mqtt_client.publish(self.topic, message)
    
    def start_loop(self):
      self.mqtt_client.loop_start()
      
    def stop_loop(self):
      self.mqtt_client.loop_stop()
