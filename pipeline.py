
import json
import apache_beam as beam
import taxfixevents_pb2 as pb
from google.protobuf.json_format import Parse
from apache_beam.options.pipeline_options import PipelineOptions


PROJECT = "events-pipeline-challenge"
TOPIC = "projects/events-pipeline-challenge/topics/tracking_events"
schema = pb.classes()


class ProtobufConversionError(Error):
    """Thrown if Error during Protobuf conversion fails."""


def send_event_data_to_storage(data, error):
    """Used to send error events to cloud storage"""

def schema_mapping(event_data):
    """
    The idea behind this function, is to quickly check if the json provided is
    correct and can be validated through a conversion into a Protocol Buffer.
    The event data is  initially sent as a default protobuf in case there is an error.
    The default Protobuf data should be something that does not affect the behavior or the KPIs in the long term.
    For the sake of simplicity I will use one of the events shared in the challenge description.
    :param event_data: Result of the Decode process, event bytes string (It's converted to this on the first stage
    of the Beam pipeline).
    :return: Protobuf ready to be parsed and written into BigQuery (Or stored
    on Google Cloud Storage in case of an error).
    """
    default_data = {
                    "id": "AED96FC7-19F1-46AB-B79F-D412117119BD",
                    "received_at": "2018-02-03 18:28:12.378000",
                    "anonymous_id": "8E0302A3-2184-4592-851D-B93C32E410AB",
                    "context_device_manufacturer": "Apple",
                    "context_device_model": "iPhone8,4",
                    "context_device_type": "ios",
                    "context_library_name": "analytics-ios",
                    "context_library_version": "3.6.7",
                    "context_locale": "de-DE",
                    "context_network_wifi": "true",
                    "context_os_name": "iOS",
                    "event":"registration_initiated",
                    "event_text": "registrationInitiated",
                    "original_timestamp": "2018 - 02 - 03T19: 28: 06.291 + 0100",
                    "sent_at":"2018 - 02 - 0318: 28: 12.000000",
                    "timestamp": "2018 - 02 - 0318: 28: 06.561000",
                    "context_network_carrier": "o2 - de",
                    "context_traits_taxfix_language": "de-DE"
                    }
    tracking_event_pb = pb.TaxfixEvent()
    event_protobuf = Parse(default_data, tracking_event_pb)

    try:
        event_data = json.loads(data)
        event_protobuf = Parse(event_data, tracking_event_pb)
    except AttributeError as e:
        raise ProtobufConversionError('Failed to convert to PB attribute: {0}.'.format(e))
        send_event_data_to_storage(event_data, e)
    except TypeError as e:
        raise ProtobufConversionError('Failed to convert to PB type: {0}.'.format(e))
        send_event_data_to_storage(event_data, e)
    except ValueError as e:
        raise ProtobufConversionError('Failed to convert to JSON: {0}.'.format(e))
        send_event_data_to_storage(event_data, e)
    return event_protobuf


class Map(beam.DoFn):
    """
    This class runs a function to do parallel processing using Beam
    It Maps the already verified event and loads it into a dictionary
    so loading to BigQuery can be simpler.
    """
    def process(self, element):
         return [{
            'id': element.id,
            'received_at': element.received_at,
            'anonymous_id': element.anonymous_id,
            'context_app_version': element.context_app_version,
            'context_device_ad_tracking_enabled': element.context_device_ad_tracking_enabled,
            'context_device_manufacturer': element.context_device_manufacturer,
            'context_device_model': element.context_device_model,
            'context_device_type': element.context_device_type,
            'context_library_name': element.context_library_name,
            'context_library_version': element.context_library_version,
            'context_locale': element.context_locale,
            'context_network_wifi': element.context_network_wifi,
            'context_os_name': element.context_os_name,
            'event': element.event,
            'event_text': element.event_text,
            'original_timestamp': element.original_timestamp,
            'sent_at': element.sent_at,
            'timestamp': element.timestamp,
            'user_id': element.user_id,
            'context_network_carrier': element.context_network_carrier,
            'context_device_token': element.context_device_token,
            'context_traits_taxfix_language': element.context_traits_taxfix_language
        }]


if __name__ == '__main__':

    p = beam.Pipeline(options=PipelineOptions())

    (p
     | "Read data" >> beam.io.ReadFromPubSub(topic=TOPIC).with_output_types(bytes)
     | "Decode" >> beam.Map(lambda x: x.decode('utf-8'))
     | "Map data to Schema" >> beam.Map(schema_mapping)
     | "Parse Protobuf" >> beam.ParDo(Map())
     | "Load on BigQuery" >> beam.io.WriteToBigQuery('{0}:userlogs.logdata'.format(PROJECT), schema=schema,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
     )
    result = p.run()
    result.wait_until_finish()