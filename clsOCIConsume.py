# By Nishant Iyer
import oci
import sys
import time
import os
from base64 import b64encode, b64decode
import json
from clsConfig import clsConfig as cf
from oci.config import from_file
import pandas as p

class clsOCIConsume:
    def __init__(self):
        self.comp = str(cf.conf['comp'])
        self.STREAM_NAME = str(cf.conf['STREAM_NAME'])
        self.PARTITIONS = int(cf.conf['PARTITIONS'])
        self.limRec = int(cf.conf['limRec'])

    def get_cursor_by_partition(self, client, stream_id, partition):
        print("Creating a cursor for partition {}".format(partition))
        cursor_details = oci.streaming.models.CreateCursorDetails(
            partition=partition,
            type=oci.streaming.models.CreateCursorDetails.TYPE_TRIM_HORIZON)
        response = client.create_cursor(stream_id, cursor_details)
        cursor = response.data.value
        return cursor

    def simple_message_loop(self, client, stream_id, initial_cursor):
        try:
            cursor = initial_cursor
            while True:
                get_response = client.get_messages(stream_id, cursor, limit=10)
                if not get_response.data:
                    return

                # Process the messages
                print(" Read {} messages".format(len(get_response.data)))
                for message in get_response.data:
                    print("{}: {}".format(b64decode(message.key.encode()).decode(),
                                          b64decode(message.value.encode()).decode()))

                
                time.sleep(1)
                cursor = get_response.headers["opc-next-cursor"]

            return 0
        except Exception as e:
            x = str(e)
            print('Error: ', x)

            return 1

    def get_stream(self, admin_client, stream_id):
        return admin_client.get_stream(stream_id)

    def get_or_create_stream(self, client, compartment_id, stream_name, partition, sac_composite):
        try:

            list_streams = client.list_streams(compartment_id=compartment_id, name=stream_name,
                                               lifecycle_state=oci.streaming.models.StreamSummary.LIFECYCLE_STATE_ACTIVE)

            if list_streams.data:
                print("An active stream {} has been found".format(stream_name))
                sid = list_streams.data[0].id
                return self.get_stream(sac_composite.client, sid)

            print(" No Active stream  {} has been found; Creating it now. ".format(stream_name))
            print(" Creating stream {} with {} partitions.".format(stream_name, partition))

            stream_details = oci.streaming.models.CreateStreamDetails(name=stream_name, partitions=partition,
                                                                      compartment_id=compartment, retention_in_hours=24)

            response = sac_composite.create_stream_and_wait_for_state(stream_details, wait_for_states=[oci.streaming.models.StreamSummary.LIFECYCLE_STATE_ACTIVE])
            return response
        except Exception as e:
            print(str(e))

    def consumeStream(self):
        try:
            STREAM_NAME = self.STREAM_NAME
            PARTITIONS = self.PARTITIONS
            compartment = self.comp
            print('Consuming sream from Oracle Cloud!')
            # Load the default configuration
            config = from_file(file_location="~/.oci/config.poc")

            
            stream_admin_client = oci.streaming.StreamAdminClient(config)
            stream_admin_client_composite = oci.streaming.StreamAdminClientCompositeOperations(stream_admin_client)

            
            stream = self.get_or_create_stream(stream_admin_client, compartment, STREAM_NAME,
                                          PARTITIONS, stream_admin_client_composite).data

            print(" Created Stream {} with id : {}".format(stream.name, stream.id))

            
            stream_client = oci.streaming.StreamClient(config, service_endpoint=stream.messages_endpoint)
            s_id = stream.id

            

            print("Starting a simple message loop with a partition cursor")
            partition_cursor = self.get_cursor_by_partition(stream_client, s_id, partition="0")
            self.simple_message_loop(stream_client, s_id, partition_cursor)

            return 0

        except Exception as e:
            x = str(e)
            print(x)

            logging.info(x)

            return 1
