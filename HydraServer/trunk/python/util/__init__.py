import logging
log = logging.getLogger(__name__)

def generate_data_hash(dataset_dict):

    d = dataset_dict
    if d.get('metadata') is None:
        d['metadata'] = {}

    hash_string = "%s %s %s %s %s, %s"%(d['data_name'],
                                    d['data_units'],
                                    d['data_dimen'],
                                    d['data_type'],
                                    d['value'],
                                    d['metadata'])

    log.debug("Generating data hash from: %s", hash_string)

    data_hash  = hash(hash_string)

    log.debug("Data hash: %s", data_hash)

    return data_hash
