import pytz
from datetime import datetime
import json
import numpy as np

def parse_id(id_):
    
    if id_.bit_length() > 40:
        bs = bin(id_)
        sequence = int('0b' + bs[-12:], 2)
        worker = int('0b' + bs[-17:-12], 2)
        dc = int('0b' + bs[-22:-17], 2)
        ts = ((id_ >> 22) + 1288834974657) / 1000.0 
        utc_dt = datetime.utcfromtimestamp(ts) 
        dt = utc_dt.replace(tzinfo=pytz.utc).strftime('%Y-%m-%d %H:%M:%S')
    else:
        dt = dc = worker = sequence = np.nan

    return str(dt), str(dc), str(worker), str(sequence), str(id_)


if __name__ == "__main__":

    OUTFILE = 'user_ids.csv'
    INFILE = '../../../data/de_stream.json'
    with open(INFILE, 'r', encoding='utf-8') as infile,\
         open(OUTFILE, 'w') as outfile:
        line_templ = '{},{},{},{}\n'
        outfile.write(line_templ.format('id_timestamp,data_center,worker,'
                                        'sequence,id_',
                                        'created_at','id_store_type','bits'))

        for i,line in enumerate(infile):
            tweet = json.loads(line)
            uid = tweet['user']['id']
            if isinstance(uid, dict):
                id_store_type = 1
                uid = int(uid['$numberLong'])
            else:
                id_store_type = 0

            created_at = tweet['user']['created_at']
            a = ','.join(parse_id(uid))
            bits = uid.bit_length()
            out = line_templ.format(a, created_at, id_store_type, bits)
            outfile.write(out)
            if i % 10000 == 0:
                print(i)
            

