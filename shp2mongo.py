import argparse
from datetime import datetime
import pymongo
import shapefile


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-H', '--host', default='localhost')
    parser.add_argument('-P', '--port', default=27017, type=int)
    parser.add_argument('-d', '--database', required=True)
    parser.add_argument('-c', '--collection', required=True)
    parser.add_argument('-C', '--column', default='shape')
    parser.add_argument('-i', '--id', default='objectid')
    parser.add_argument('-e', '--encoding', required=True)
    parser.add_argument('--drop', action='store_true')
    parser.add_argument('file')
    args = parser.parse_args()

    d1 = datetime.now()

    client = pymongo.MongoClient(args.host, args.port)
    database = client.get_database(args.database)
    if args.drop:
        database.drop_collection(args.collection)
    collection = database.get_collection(args.collection)

    reader = shapefile.Reader(args.file)
    # skip 'DeletionFlag'
    fields = reader.fields[1:]
    field_names = [d[0] for d in fields]
    field_types = [d[1] for d in fields]

    def decode_maybe(value, field_type):
        if field_type == 'C':
            # pyshp return incorrect value if encoding of dbf is not 'utf-8'
            return value.decode(args.encoding)
        else:
            return value

    for idx, feature in enumerate(reader.iterShapeRecords(), start=1):
        values = map(decode_maybe, feature.record, field_types)
        doc = dict(zip(field_names, values))
        doc[args.id] = idx
        doc[args.column] = feature.shape.__geo_interface__
        collection.insert_one(doc)

    if args.id != '_id':
        collection.create_index(args.id)
    try:
        collection.create_index([(args.column, pymongo.GEOSPHERE)])
    finally:
        client.close()

    d2 = datetime.now()
    print 'Time elapsed: %s' % str(d2 - d1)


if __name__ == "__main__":
    main()
