import re
import aiohttp
import decimal
import unicodedata
import os
import glob
import fnmatch

from hashlib import md5
from .constants import DISCORD_MSG_CHAR_LIMIT


def song_list(osudir):
    osus = []
    songs = []
    for song in os.listdir(osudir+'\Songs'):
        print('Progressing '+ song)
        if len(glob.glob(osudir + '\Songs\\' + song + '\*.mp3')) == 1:
            osus.append(glob.glob(osudir + '\Songs\\' + song + '\*.osu')[0])
        else:
            for osu in glob.glob(osudir + '\Songs\\' + song + '\*.osu'):
                try:
                    with open(osu, encoding='utf8') as f:
                        for line in f:
                            line = line.strip()
                            if line and line.startswith('AudioFilename: '):
                                file = osudir + '\Songs\\' + song + '\\' + line[15:len(line)]
                                if file not in songs:
                                    osus.append(osu)
                                songs.append(file)
                                break
                except IOError as e:
                    print("Error loading", song, e)
    return osus

def load_file(filename, skip_commented_lines=True, comment_char='#'):
    try:
        with open(filename, encoding='utf8') as f:
            results = []
            for line in f:
                line = line.strip()

                if line and not (skip_commented_lines and line.startswith(comment_char)):
                    results.append(line)

            return results

    except IOError as e:
        print("Error loading", filename, e)
        return []


def write_file(filename, contents):
    with open(filename, 'w', encoding='utf8') as f:
        for item in contents:
            f.write(str(item))
            f.write('\n')


def slugify(value):
    value = unicodedata.normalize('NFKD', value).encode('ascii', 'ignore').decode('ascii')
    value = re.sub('[^\w\s-]', '', value).strip().lower()
    return re.sub('[-\s]+', '-', value)


def sane_round_int(x):
    return int(decimal.Decimal(x).quantize(1, rounding=decimal.ROUND_HALF_UP))


def paginate(content, *, length=DISCORD_MSG_CHAR_LIMIT, reserve=0):
    """
    Split up a large string or list of strings into chunks for sending to discord.
    """
    if type(content) == str:
        contentlist = content.split('\n')
    elif type(content) == list:
        contentlist = content
    else:
        raise ValueError("Content must be str or list, not %s" % type(content))

    chunks = []
    currentchunk = ''

    for line in contentlist:
        if len(currentchunk) + len(line) < length - reserve:
            currentchunk += line + '\n'
        else:
            chunks.append(currentchunk)
            currentchunk = ''

    if currentchunk:
        chunks.append(currentchunk)

    return chunks


async def get_header(session, url, headerfield=None, *, timeout=5):
    with aiohttp.Timeout(timeout):
        async with session.head(url) as response:
            if headerfield:
                return response.headers.get(headerfield)
            else:
                return response.headers


def md5sum(filename, limit=0):
    fhash = md5()
    with open(filename, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            fhash.update(chunk)
    return fhash.hexdigest()[-limit:]
