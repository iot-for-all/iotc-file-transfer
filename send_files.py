import os
import base64
import hmac
import hashlib
import zlib
import uuid
import math

from azure.iot.device import ProvisioningDeviceClient
from azure.iot.device import IoTHubDeviceClient
from azure.iot.device import Message
from azure.iot.device import exceptions

# device settings - FILL IN YOUR VALUES HERE
scope_id = "<scope id for IoT Central Application"
group_symmetric_key = "Group SAS key for IoT Central Application"

# optional device settings - CHANGE IF DESIRED/NECESSARY
provisioning_host = "global.azure-devices-provisioning.net"
device_id = "file-transfer-device-01"
model_id = "dtmi:sample:fileTransfer;1"  # This model is available in the root of the Github repo (file-transfer-device-template.json) and can be imported into your Azure IoT central application

# general purpose variables
use_websockets = True
device_client = None
terminate = False
trying_to_connect = False
max_connection_attempt = 3
verbose_logging = False

multipart_msg_schema = '{{"data": "{}"}}'

# derives a symmetric device key for a device id using the group symmetric key
def derive_device_key(device_id, group_symmetric_key):
    message = device_id.encode("utf-8")
    signing_key = base64.b64decode(group_symmetric_key.encode("utf-8"))
    signed_hmac = hmac.HMAC(signing_key, message, hashlib.sha256)
    device_key_encoded = base64.b64encode(signed_hmac.digest())
    return device_key_encoded.decode("utf-8")

# Send a file over the IoT Hub transport to IoT Central
def send_file(filename, upload_filepath, compress):
    f = open(filename, "rb")
    data = f.read()
    file_size_kb = len(data) / 1024
    f.close()

    # decide if to compress the data using zlib deflate
    if compress:
        data_compressed = zlib.compress(data)
    else:
        data_compressed = data

    # encode the data with base64 for transmission as a JSON string
    data_base64 = base64.b64encode(data_compressed).decode("ASCII")

    max_msg_size = 255 * 1024
    msg_template_size = len(multipart_msg_schema)
    max_content_size = max_msg_size - msg_template_size
    
    max_parts = int(math.ceil(len(data_base64) / max_content_size))
    file_id = uuid.uuid4()
    part = 1
    index = 0

    # chunk the file payload into 255KB chunks to send to IoT central over MQTT (could also be AMQP or HTTPS)
    status = 200
    status_message = "completed"
    for i in range(max_parts):
        data_chunk = data_base64[index:index + max_content_size]
        index = index + max_content_size

        payload = multipart_msg_schema.format(data_chunk)

        if device_client and device_client.connected:
            if verbose_logging:
                print("Start sending multi-part message: %s" % (payload))
            else:
                print("Start sending multi-part message")

            msg = Message(payload)
            
            # standard message properties
            msg.content_type = "application/json";  # when we support binary payload this should be changed to application/octet-stream
            msg.content_encoding = "utf-8"; # encoding for the payload utf-8 for JSON and can be left off for binary data

            # custom message properties
            msg.custom_properties["multipart-message"] = "yes";  # indicates this is a multi-part message that needs special processing
            msg.custom_properties["id"] = file_id;   # unique identity for the multi-part message we suggest using a UUID
            msg.custom_properties["filepath"] = upload_filepath; # file path for the final file, the path will be appended to the base recievers path
            msg.custom_properties["part"] = str(part);   # part N to track ordring of the parts
            msg.custom_properties["maxPart"] = str(max_parts);   # maximum number of parts in the set
            compression_value = "none"
            if compress:
                compression_value = "deflate"
            msg.custom_properties["compression"] = compression_value;   # use value 'deflate' for compression or 'none'/remove this property for no compression
            
            try:
                device_client.send_message(msg)
                print("completed sending multi-part message")
            except Exception as err:
                status_message = "Received exception during send_message. Exception: " + err
                print(status_message)
                status = 500
            
        part = part + 1

    # send a file transfer status message to IoT Central over MQTT
    payload = f'{{"filename": "{filename}", "filepath": "{upload_filepath}", "status": {status}, "message": "{status_message}", "size": {file_size_kb}}}'
    msg = Message(payload)
            
    # standard message properties
    msg.content_type = "application/json";  # when we support binary payload this should be changed to application/octet-stream
    msg.content_encoding = "utf-8"; # encoding for the payload utf-8 for JSON and can be left off for binary data

    device_client.send_message(msg)
    print("completed sending file transfer status message")


# connect is not optimized for caching the IoT Hub hostname so all connects go through Device Provisioning Service (DPS)
# a strategy here would be to try just the hub connection using a cached IoT Hub hostname and if that fails fall back to a full DPS connect
def connect():
    global device_client

    device_symmetric_key = derive_device_key(device_id, group_symmetric_key)

    connection_attempt_count = 0
    connected = False
    while not connected and connection_attempt_count < max_connection_attempt:
        provisioning_device_client = ProvisioningDeviceClient.create_from_symmetric_key(
            provisioning_host=provisioning_host,
            registration_id=device_id,
            id_scope=scope_id,
            symmetric_key=device_symmetric_key,
            websockets=use_websockets
        )

        provisioning_device_client.provisioning_payload = '{"iotcModelId":"%s"}' % (model_id)
        registration_result = None

        try:
            registration_result = provisioning_device_client.register()
        except (exceptions.CredentialError, exceptions.ConnectionFailedError, exceptions.ConnectionDroppedError, exceptions.ClientError, Exception) as e:
            print("DPS registration exception: " + e)
            connection_attempt_count += 1

        if registration_result.status == "assigned":
            dps_registered = True

        if dps_registered:
            device_client = IoTHubDeviceClient.create_from_symmetric_key(
                symmetric_key=device_symmetric_key,
                hostname=registration_result.registration_state.assigned_hub,
                device_id=registration_result.registration_state.device_id,
                websockets=use_websockets
            )

        try:
            device_client.connect()
            connected = True
        except Exception as e:
            print("Connection failed, retry %d of %d" % (connection_attempt_count, max_connection_attempt))
            connection_attempt_count += 1

    return connected


def main():
    # Connect to IoT hub/central
    print("Connecting to IoT Hub/Central")
    if connect():
        local_upload_dir = "./sample-upload-files/"

        for i in range(0, 10):
            # send an mp4 video file with compression
            send_file(local_upload_dir + "video.mp4", "myDevice\\video\\video.mp4", True)

            # send a pdf file with compression
            send_file(local_upload_dir + "large-pdf.pdf", "myDevice\\pdf\\large-pdf.pdf", True) # 10,386KB

            # send a jpg file without compression
            send_file(local_upload_dir + "4k-image.jpg", "myDevice/images/4k-image.jpg", False) # 3,914KB

        # disconnect from IoT hub/central
        print("Disconnecting from IoT Hub/Central")
        device_client.disconnect()
    else:
        print('Cannot connect to Azure IoT Central please check the application settings and machine connectivity')


# start the main routine
if __name__ == "__main__":
    main()
