"""Delta Sharing: catalog tables served to outside tools over the open Delta Sharing protocol.

``service`` manages shares, their tables and the API consumers granted them; ``server`` answers the protocol for
a recipient; ``log_reader`` turns a table's Delta log into the protocol's lines; ``presign`` hands out the file
URLs a client downloads, through ``file_tokens`` for a table on local disk. Core reads only the Delta log and
streams file bytes as stored, so nothing here materialises a frame.
"""
