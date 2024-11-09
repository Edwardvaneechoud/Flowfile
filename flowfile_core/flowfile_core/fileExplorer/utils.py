import os
import re

video_types = ['mp4', "webm", "opgg"]
audio_types = ['mp3', "wav", "ogg", "mpeg", "aac", "3gpp", "3gpp2", "aiff", "x-aiff", "amr", "mpga"]


def get_chunk(start_byte=None, end_byte=None, full_path=None):
    file_size = os.stat(full_path).st_size
    if end_byte:
        length = end_byte + 1 - start_byte
    else:
        length = file_size - start_byte
    with open(full_path, 'rb') as f:
        f.seek(start_byte)
        chunk = f.read(length)
    return chunk, start_byte, length, file_size


def get_file(file_path, mimetype):
    f = ""
    range_header = f.headers.get('Range', None)
    start_byte, end_byte = 0, None
    if range_header:
        match = re.search(r'(\d+)-(\d*)', range_header)
        groups = match.groups()
        if groups[0]:
            start_byte = int(groups[0])
        if groups[1]:
            end_byte = int(groups[1])

    chunk, start, length, file_size = get_chunk(start_byte, end_byte, file_path)
    resp = Response(chunk, 206, mimetype=f'video/{mimetype}',
                    content_type=mimetype, direct_passthrough=True)
    print(length)
    resp.headers.add('Content-Range', 'bytes {0}-{1}/{2}'.format(start, start + length - 1, file_size))
    return resp


def is_media(filepath):
    found_media = re.search("\.mp4$|\.mp3$", filepath, re.IGNORECASE)
    if found_media:
        extension = found_media[0].lower()[1:]
        if found_media in video_types:
            return f"video/{extension}"
        return f"audio/{extension}"
    return False


def get_file_extension(fname):
    found_extension = re.search("\.[A-Za-z0-9]*$", fname, re.IGNORECASE)
    if found_extension:
        return found_extension[0][1:].lower()