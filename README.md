# iotc-file-transfer

## Description

Many devices need to push large payloads up to IoT Central and this has typically been achieved by using the File Upload feature in IoT Central that is backed by the "File Upload" feature in IoT Hub.  This works by configuring an Azure BLOB Storage container with IoT Central then using the Azure IoT device SDK to get a key to that container allowing the device to open an HTTPS connection to the BLOB storage container and pushing up the large payload.  Whilst this is an effective and secure way to push large payloads it does require the device to open a second secure connection to the Azure cloud something that can be a challenge for constrained devices.

This sample shows how you can push large payloads up to IoT central using the standard IoT transports (MQTT, AMQP, HTTPS) and without the need to open a second connection.  Using the IoT Central Continuous Data Export (CDE) feature and a simple Azure function it is possible to push very large payloads up to Azure.

## How it works

