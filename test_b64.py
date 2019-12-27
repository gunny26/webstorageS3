#!/usr/bin/python3
import base64
from binascii import b2a_base64
filename = "some very special!&°'''"
print("utf-8 bytes : ", filename.encode("utf-8").hex())
print("filename    : ", filename)
filename64 = base64.b64encode(filename.encode("utf-8"))
print("base64      : ", filename64)
filename_hex = filename64.hex()
print("hex(base64) : ", filename_hex)
r_filename64 = bytes.fromhex(filename_hex)
print("re-base64   : ", r_filename64)
r_filename = base64.b64decode(r_filename64).decode("utf-8")
print("re-filename : ", r_filename)
