import zmq
import json
from loguru import logger
import numpy as np

def dict_flatten(in_dict, dict_out=None, parent_key=None, separator="_"):
    """flattens dict to serialize"""
    if dict_out is None:
        dict_out = {}

    for k, v in in_dict.items():
        k = f"{parent_key}{separator}{k}" if parent_key else k
        if isinstance(v, dict):
            dict_flatten(in_dict=v, dict_out=dict_out, parent_key=k)
            continue

        elif array_flatten(v, dict_out, k):
            continue

        dict_out[k] = v

    return dict_out

def array_flatten(unflatten, in_dict, dict_key):

    if isinstance(unflatten, np.ndarray):
        unflatten = unflatten.flatten().tolist()

    if isinstance(unflatten, list):
        for _it, _value in enumerate(unflatten):
            if isinstance(_value, list):
                array_flatten(_value, in_dict, f"{dict_key}_{_it}")
            else:
                in_dict[f"{dict_key}_{_it}"] = _value
        return 1
    
    return 0


class NpEncoder(json.JSONEncoder):
    """Object to serialize our dicts into json file"""
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.dtype):
            return str(obj)  
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        else:
            return super(NpEncoder, self).default(obj)   


def NpTypeEncoder(in_dict, out_dict=None, parent_key=None, separator="/"):
    """Stores numpy dtype information in dicts to be serialized"""
    if out_dict is None:
        out_dict = {}

    for k, v in in_dict.items():
        k = f"{parent_key}{separator}{k}" if parent_key else k

        if isinstance(v, dict):
            NpTypeEncoder(in_dict=v, out_dict=out_dict, parent_key=k)
            continue

        if isinstance(v, np.ndarray):
            out_dict[f"{k}_dtype"]= v.dtype

        out_dict[k] = v
        
    return out_dict

    
def NpTypeDecoder(json_loads):
    """Recreates dict with received numpy dtypes."""
    out_dict = {}

    for k, v in json_loads.items():
        if "_dtype" in k:
            out_dict[k[:-6]] = v
        elif k in out_dict.keys():
            out_dict[k] = np.array(v, dtype=out_dict[k])
        else:
            out_dict[k] = v

    return out_dict