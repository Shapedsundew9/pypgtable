from db_raw_table import db_raw_table
from json import dumps, loads
from zlib import compress, decompress


# Matrix of functions to convert between types
_IDENTITY_FUNC = lambda x: x
_TO_JSON_STR_FUNC = lambda x: dumps(x)
_FROM_JSON_STR_FUNC = lambda x: loads(x)
_TO_JSON_STR_ZIP_FUNC = lambda x: compress(dumps(x))
_FROM_JSON_STR_ZIP_FUNC = lambda x: loads(decompress(x))
_CAST_MATRIX = {
	'dict': {
		'json_str': _TO_JSON_STR_FUNC,
		'json_str_zip' : _TO_JSON_STR_ZIP_FUNC
	},
	'list': {
		'json_str': _TO_JSON_STR_FUNC,
		'json_str_zip' : _TO_JSON_STR_ZIP_FUNC
	},
	'json_str': {
		'dict': _FROM_JSON_STR_FUNC,
		'list': _FROM_JSON_STR_FUNC
	},
	'json_str_zip': {
		'dict': _FROM_JSON_STR_ZIP_FUNC,
		'list': _FROM_JSON_STR_ZIP_FUNC
	}
}
	

class db_table():
	
	def __init__(self, config):
		self._raw_table = db_raw_table(config)
		self._select_conversions = {column: _CAST_MATRIX.get(_type[0], {_type[1]: _IDENTITY_FUNC})][_type[1]] for column, _type in config['columns']} 

		
	def select(self, query, fields):
		return [{field: self._select_conversions[field](value) for field, value in zip(fields, row)} for row in self._raw_table.select(query, fields)]
	
    def _cast_term_to_store_type(self, term, value):
        # FIXME: Compressed object not necessarily JSON
        if self.schema[term]['compressed']: value = compress(dumps(value), 9)
        elif self.schema[term]['database']['type'] == "BYTEA":
            if self.config['schema'][term]['type'] == 'string': value = bytearray.fromhex(value)
            # elif self.config['schema'][term]['type'] == 'binary': noop
            elif self.schema[term]['bitarray']: value = value.to_bytes()
        elif self.schema[term]['database']['type'] == "TIMESTAMP":
            if self.config['schema'][term]['type'] == 'datetime': value = value.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            # elif self.config['schema'][k]['type'] == 'string': noop
        elif 'codec' in self.schema[term]:
            if isinstance(value, dict):
                bitfield = 0x0000000000000000
                for k, v in value.items():
                    if v: bitfield |= 1 << self.schema[term]['codec'][k]
                value = bitfield
        return value


    def _cast_entry_to_store_type(self, e):
        entry = deepcopy(e)
        for k, v in filter(lambda x: x[0] in self.schema, entry.items()): entry[k] = self._cast_term_to_store_type(k, v)
        return entry
	



    def _cast_entry_to_load_type(self, data, fields):
        entry = dict(zip(fields, data))
        for k, v in filter(lambda x: x[0] in self.schema and not x[1] is None, entry.items()):
            # FIXME: A compressed field does not have to be JSON.
            schema_k = self.schema[k]
            if schema_k['compressed']: entry[k] = loads(decompress(v))
            elif isinstance(v, memoryview):
                if schema_k['type'] == 'string': entry[k] = v.hex()
                elif schema_k['type'] == 'binary': entry[k] = v
                elif schema_k['bitarray']: entry[k] = bitarray(v)
            elif isinstance(v, datetime):
                if schema_k['type'] == 'string': entry[k] = v.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                if schema_k['type'] == 'datetime': entry[k] = v
            elif 'codec' in self.schema[k]: entry[k] = {b: bool((1 << f) & entry[k]) for b, f in self.schema[k]['codec'].items() }
        return entry