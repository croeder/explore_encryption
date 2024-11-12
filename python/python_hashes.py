#!/usr/bin/env python3

import hashlib

test_string = 'a|123445|chris|roeder|123 S. Main St|Ft Morgan|CO|80000' 

def create_hash(input_string):
    """ matches common SQL code when that code also truncates to 13 characters
        SQL: cast(conv(substr(md5(test_string), 1, 15), 16, 10) as bigint) as hashed_value
    """
    
    hash_value = hashlib.sha256(test_string.encode('utf-8'))
    print(f"{input_string}")
    print(f"    {hash_value.hexdigest()}")
    
    hash_value2 = hashlib.sha256(test_string.encode('ascii'))
    print(f"{input_string}")
    print(f"    {hash_value2.hexdigest()}")
    
    hash_value3 = hashlib.sha256(test_string.encode('utf-16'))
    print(f"{input_string}")
    print(f"    {hash_value3.hexdigest()}")
    
    hash_value4 = hashlib.sha256(test_string.encode('latin-1'))
    print(f"{input_string}")
    print(f"    {hash_value4.hexdigest()}")
    
# not old, but PySpark
#    old_hash = sha2(test_string.encode('utf-8'), 256)
#    print(f"    {old_hash}")
    
    
    truncated_hash = hash_value.hexdigest()[0:13]
    int_trunc_hash_value = int(truncated_hash, 16)
    return int_trunc_hash_value

print(create_hash(test_string))
