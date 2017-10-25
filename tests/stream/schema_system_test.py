import sys
import pytest
from data_pipeline.stream.schema import SchemaRegistry

def main(schema_registry_url):
    schema_registry = SchemaRegistry(schema_registry_url)
    response = schema_registry.get_schema(id=121)
    print response.json()

    response = schema_registry.get_subjects()
    print response.json()

    response = schema_registry.get_versions('Kafka-value')
    print response.json()

    response = schema_registry.get_schema(subject='Kafka-value', version='latest')
    print response.json()

    schema = {"schema": '{"type": "record", "name": "myrecord", "fields":[{"name":"f1","type":"string"}]}}'}
    response = schema_registry.create_schema('Kafka-value', schema)
    print response.json()['id']

    schema = {"schema": '{"type": "record", "name": "myrecord", "fields":[{"name":"f1","type":"string"}]}}'}
    response = schema_registry.schema_exists('Kafka-value', schema)
    print response.json()

    schema = {"schema": '{"type": "record", "name": "myrecord", "fields":[{"name":"f2","type":"string"}]}}'}
    response = schema_registry.schema_exists('Kafka-value', schema)
    print response.json()

    schema = {"schema": '{"type": "record", "name": "myrecord", "fields":[{"name":"f1","type":"string"}]}}'}
    response = schema_registry.schema_exists('Non-existent-subject', schema)
    print response.json()

    schema = {"schema": '{"type": "record", "name": "myrecord", "fields":[{"name":"f1","type":"string"}]}}'}
    response = schema_registry.schema_compatible('Kafka-value', 1, schema)
    print response.json()

    config = {"compatibility": "FULL",}
    response = schema_registry.update_config(config=config)
    print response.json()

    config = {"compatibility": "FULL",}
    response = schema_registry.update_config(subject="Kafka-value", config=config)
    print response.json()

    response = schema_registry.get_config()
    print response.json()

if __name__ == "__main__":
    main(sys.argv[1])
