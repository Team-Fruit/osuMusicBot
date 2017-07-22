import datetime
import traceback
from collections import deque
from itertools import islice
from random import shuffle

from .utils import get_header
from .entry import URLPlaylistEntry
from .exceptions import ExtractionError, WrongEntryTypeError
from .lib.event_emitter import EventEmitter


class Playlist(EventEmitter):
    """
        A playlist is manages the list of songs that will be played.
    """

    def __init__(self, bot):
        super().__init__()
        self.bot = bot
        self.loop = bot.loop
        self.downloader = bot.downloader
        self.entries = deque()

    def __iter__(self):
        return iter(self.entries)

    def shuffle(self):
        shuffle(self.entries)

    def clear(self):
        self.entries.clear()

    async def import_from(self, playlist_url, **meta):
        """
            Imports the songs from `playlist_url` and queues them to be played.

            Returns a list of `entries` that have been enqueued.

            :param playlist_url: The playlist url to be cut into individual urls and added to the playlist
            :param meta: Any additional metadata to add to the playlist entry
        """
        position = len(self.entries) + 1
        entry_list = []

        try:
            info = await self.downloader.safe_extract_info(self.loop, playlist_url, download=False)
        except Exception as e:
            raise ExtractionError('Could not extract information from {}\n\n{}'.format(playlist_url, e))

        if not info:
            raise ExtractionError('Could not extract information from %s' % playlist_url)

        # Once again, the generic extractor fucks things up.
        if info.get('extractor', None) == 'generic':
            url_field = 'url'
        else:
            url_field = 'webpage_url'

        baditems = 0
        for items in info['entries']:
            if items:
                try:
                    entry = URLPlaylistEntry(
                        self,
                        items[url_field],
                        items.get('title', 'Untitled'),
                        items.get('duration', 0) or 0,
                        self.downloader.ytdl.prepare_filename(items),
                        **meta
                    )

                    self.add_entry(entry)
                    entry_list.append(entry)
                except:
                    baditems += 1
                    # Once I know more about what's happening here I can add a proper message
                    traceback.print_exc()
                    print(items)
                    print("Could not add item")
            else:
                baditems += 1

        if baditems:
            print("Skipped %s bad entries" % baditems)

        return entry_list, position

    async def async_process_youtube_playlist(self, playlist_url, **meta):
        """
            Processes youtube playlists links from `playlist_url` in a questionable, async fashion.

            :param playlist_url: The playlist url to be cut into individual urls and added to the playlist
            :param meta: Any additional metadata to add to the playlist entry
        """

        try:
            info = await self.downloader.safe_extract_info(self.loop, playlist_url, download=False, process=False)
        except Exception as e:
            raise ExtractionError('Could not extract information from {}\n\n{}'.format(playlist_url, e))

        if not info:
            raise ExtractionError('Could not extract information from %s' % playlist_url)

        gooditems = []
        baditems = 0
        for entry_data in info['entries']:
            if entry_data:
                baseurl = info['webpage_url'].split('playlist?list=')[0]
                song_url = baseurl + 'watch?v=%s' % entry_data['id']

                try:
                    entry, elen = await self.add_entry(song_url, **meta)
                    gooditems.append(entry)
                except ExtractionError:
                    baditems += 1
                except Exception as e:
                    baditems += 1
                    print("There was an error adding the song {}: {}: {}\n".format(
                        entry_data['id'], e.__class__.__name__, e))
            else:
                baditems += 1

        if baditems:
            print("Skipped %s bad entries" % baditems)

        return gooditems

    async def async_process_sc_bc_playlist(self, playlist_url, **meta):
        """
            Processes soundcloud set and bancdamp album links from `playlist_url` in a questionable, async fashion.

            :param playlist_url: The playlist url to be cut into individual urls and added to the playlist
            :param meta: Any additional metadata to add to the playlist entry
        """

        try:
            info = await self.downloader.safe_extract_info(self.loop, playlist_url, download=False, process=False)
        except Exception as e:
            raise ExtractionError('Could not extract information from {}\n\n{}'.format(playlist_url, e))

        if not info:
            raise ExtractionError('Could not extract information from %s' % playlist_url)

        gooditems = []
        baditems = 0
        for entry_data in info['entries']:
            if entry_data:
                song_url = entry_data['url']

                try:
                    entry, elen = await self.add_entry(song_url, **meta)
                    gooditems.append(entry)
                except ExtractionError:
                    baditems += 1
                except Exception as e:
                    baditems += 1
                    print("There was an error adding the song {}: {}: {}\n".format(
                        entry_data['id'], e.__class__.__name__, e))
            else:
                baditems += 1

        if baditems:
            print("Skipped %s bad entries" % baditems)

        return gooditems

    async def add_entry(self, entry):
        self.entries.append(entry)
        self.emit('entry-added', playlist=self, entry=entry)

        if self.peek() is entry:
            entry.get_ready_future()

    async def get_next_entry(self, predownload_next=True):
        """
            A coroutine which will return the next song or None if no songs left to play.

            Additionally, if predownload_next is set to True, it will attempt to download the next
            song to be played - so that it's ready by the time we get to it.
        """
        if not self.entries:
            return None

        entry = self.entries.popleft()

        if predownload_next:
            next_entry = self.peek()
            if next_entry:
                next_entry.get_ready_future()

        return await entry.get_ready_future()

    def peek(self):
        """
            Returns the next entry that should be scheduled to be played.
        """
        if self.entries:
            return self.entries[0]

    async def estimate_time_until(self, position, player):
        """
            (very) Roughly estimates the time till the queue will 'position'
        """
        estimated_time = sum([e.duration for e in islice(self.entries, position - 1)])

        # When the player plays a song, it eats the first playlist item, so we just have to add the time back
        if not player.is_stopped and player.current_entry:
            estimated_time += player.current_entry.duration - player.progress

        return datetime.timedelta(seconds=estimated_time)

    def count_for_user(self, user):
        return sum(1 for e in self.entries if e.meta.get('author', None) == user)


