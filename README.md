Google Cloud streaming pipeline
===============================

Schema validation overview
--------------------------

The basis of this schema validation is the use of Protocol Buffers (Protobuf).
Protobufs are a mechanism to serialize structured data, and can be easily used
to create a language agnostic schema using a single .proto file.

I have decided to use this tool due to it's flexibility and simplicity.
It also has many benefits when working with data streams since it allows for a
backwards and forwards compatible evolution of their streaming system, which in short
words means: If the data schema evolves and is being used by different teams or 
data-sources, the effort needed to update the schema is minimal when missing data or new
dat is present in the pipeline.
In conclusion, this method of schema validation will provie the following benefits:

* Resilience: Supports evolution of sources and sinks
* Maintainability: Easy to support and maintain

To install Protobufs, please follow the instructions [here](https://github.com/protocolbuffers/protobuf).
After installing the package, you can use the .proto file to generate the class file taxfixevents_pb2.py
using the following command: `protoc -python_out=./ ./taxfixevents.proto`

Schema validation detail
------------------------
The way we use Protobufs for controling data quality is by using their Schema definition as a data
mask.
This data mask will accept the content of a JSON event structured according to the schema defined
in the `taxfixevents_pb2` class.
Using error handling it is possible to quickly determine what the nature of the issue is, as well
as ensuring the pipeline is not broken and the errors can be analyzed in the future.
All failed events will have default data that whould be easy to spot and either remove or ignore.

Note: The dirty JSON cleanup Regex was taken from [this](https://grimhacker.com/2016/04/24/loading-dirty-json-with-python/) blog.


Streaming Pipeline
------------------
Here is an outline of the pipeline designed for this challenge:

1) Assumption that the tracking application will have something similar to what is described 
   in the `events_to_pubsub.py` file, to send events into a Google Pub/Sub Topic.
2) Once data is streaming through the Pub/Sub topic, we can use Google DataFlow to design a pipeline
   that can include a custom schema validation of our choice, in this case, the Protobuf mentioned earlier.
   The pipeline has the following structure:
   
    * "Read data": Read data from Pub/Sub
    * "Decode": Decode everything from UTF8 to Unicode
    * "Map data to Schema": Schema validations step
    * "Parse Protobuf": If Protobuf was successful, it will parse it into a dictionary
    * "Load on BigQuery": Loads the data into BigQuery
    
Possible improvements
---------------------
* Improve error handling to identify the exact column and value where the schema mapping failed.
* Add the flow of error events into cloud storage using Beam instead of a load function
* Add unit tests to the pipeline
* Add end-to-end data quality checks
