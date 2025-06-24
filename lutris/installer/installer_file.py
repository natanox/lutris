"""Manipulates installer files"""

import os
from io import BytesIO
from gettext import gettext as _, ngettext as __
from typing import Optional, Tuple, Dict, Set
from urllib.parse import urlparse
from threading import Lock

from gi.repository import GObject, Gtk

from lutris.cache import get_url_cache_path, has_valid_custom_cache_path, save_to_cache
from lutris.gui.widgets.download_progress_box import DownloadProgressBox
from lutris.installer.errors import ScriptingError
from lutris.util import system
from lutris.util.downloader import Downloader, is_file_available
from lutris.util.log import logger
from lutris.util.strings import gtk_safe_urls
from lutris.util.jobs import thread_namespace, check_stop, StopRequested, ProcessManager, schedule_at_idle
from lutris.installer.installer_file_source import SourceSteam, SourceDownload, SourceUser, SourceCache


class InstallerFile(GObject.Object):
    """Representation of a file in the `files` sections of an installer.
    Not to be confused with a file source, of which there can be multiple that are getting managed by this class. This is the class that handles retrival of a single file."""

    __gsignals__ = {
        "new-speed-measured": (
            GObject.SIGNAL_RUN_LAST,
            None,
            (str, float),
        ),  # (domain, speed) Measured and originally emitted by another class that isn't our own source
        "federate-speed-measured": (
            GObject.SIGNAL_RUN_LAST,
            None,
            (str, float),
        ),  # (domain, speed) Measurement to be picked up by another class that isn't our own source
        "processing-start": (GObject.SIGNAL_RUN_LAST, None, ()),  # For the the UI spinner to indicate worker processes
        "processing-stop": (GObject.SIGNAL_RUN_LAST, None, ()),
    }

    ######################################################
    ################### initialization ###################
    ######################################################

    def __init__(self, game_slug, file_id, file_meta):
        super().__init__()
        self.game_slug = game_slug
        self.id = file_id.replace("-", "_")  # pylint: disable=invalid-name
        self._file_meta = file_meta
        self._filename = ""
        self._multiple_sources = (
            False  # If false there's none or just one external source (next to user-specified files)
        )
        self._url_override = None  # Used for caching
        self._sources_lock = Lock()
        self.sources = self._read_sources_from_file(file_meta)
        self._user_source = self.get_sources_by_class(SourceUser)[
            0
        ]  # Reference to the only instance of SourceUser in self.sources
        self.cache_source = None  # Reference to the only instance of SourceCache in self.sources, if it exists
        self.active_source = None  # # Currently selected source for download if the file isn't cached
        self._first_valid_download_source = None  # Lazy storage for the first valid source for download so we don't have to iterate over the list every time
        self.select_source("|init")
        self.connect("new-speed-measured", self.on_speed_measured)
        self._dest_file_override = None  # Used to override the destination
        self._dest_file_found = None  # Lazy storage for the resolved destination file

    def _read_sources_from_file(self, file_meta):
        """Returns a list of source classes for proper data handling"""
        # Any change in how Lutris reads the "files" section of an install script should be done here
        sources = []
        used_ids = []
        if isinstance(file_meta, dict):
            self.filename = file_meta.get("filename")
            if "url" in file_meta:
                sources.append(self._new_source(file_meta, "1"))
            else:
                _dict = {k: v for k, v in file_meta.items() if isinstance(v, dict)}
                for key, value in _dict.items():
                    if isinstance(value, dict):
                        if not "url" in value:
                            raise ScriptingError(_("missing field `url` for file `%s`") % self.id)
                        _new = self._new_source(value, key if not used_ids else self._unique_id(key, used_ids))
                        sources.append(_new)
                        used_ids.append(_new.source_id)
        else:
            sources.append(self._new_source({"url": file_meta}, "1"))

        # Connect any signals here
        for source in sources:
            if isinstance(source, SourceDownload):
                source.connect("new-speed-measured", self.on_speed_measured)
                source.connect(
                    "new-speed-measured", lambda widget, url, speed: self.emit("federate-speed-measured", url, speed)
                )  # Forward signal to other files

        # Making sure there is always a user source
        if not any(isinstance(source, SourceUser) for source in sources):
            sources.append(
                SourceUser(
                    source_id=self._unique_id("user", [source.source_id for source in sources]),
                    url="N/A",
                    checksum=None,
                )
            )
        self._user_source = next((source for source in sources if isinstance(source, SourceUser)), None)

        # Checking PGA cache
        if self.uses_pga_cache() and self.is_cached:
            _new = SourceCache(
                source_id=self._unique_id("cache", [source.source_id for source in sources]),
                url=self.dest_file,
                temporary=False,
            )
            sources.append(_new)
            self.cache_source = _new

        if len([source for source in sources if not isinstance(source, SourceUser)]) > 1:
            self._multiple_sources = True
        return sources

    def _new_source(self, _dict, source_id):
        """Returns the correct source class for a given dict. Called repeatedly during initialization."""
        _url = str(_dict.get("url"))
        _referer = _dict.get("referer") or None
        _checksum = _dict.get("checksum") or None
        _alternate_filenames = _dict.get("alternate-filenames") or None  # used for GOG
        _downloader = (
            _dict.get("downloader") or None
        )  # used for itchio, see https://github.com/lutris/lutris/commit/178e894d2610b08cd287a2de20ad20a3757cbe55
        if _url.startswith("$STEAM"):
            return SourceSteam(source_id, _url, checksum=_checksum)
        elif _url.startswith("N/A"):
            return SourceUser(source_id, _url, checksum=_checksum)
        elif _url.startswith(("http", "file")):
            return SourceDownload(
                source_id, _url, checksum=_checksum, referer=_referer, alternate_filenames=_alternate_filenames
            )
        else:
            raise ValueError("Unsupported provider for %s" % _url)

    ######################################################
    ################### Source Handling ##################
    ######################################################

    def add_source(self, source) -> str:
        """Add a new file source manually. Returns the source_id if successful.
        The source_id might be changed or added if necessary. Other than that it's up to the calling function to provide a source object with valid data."""
        if isinstance(source, SourceCache) and self.cache_source:
            raise ValueError("Cache source already exists")
            return
        source.source_id = self._unique_id(source.source_id)
        with self._sources_lock:
            self.sources.append(source)
        return source.source_id

    def _unique_id(self, source_id, id_list=None) -> str:
        """In case of collisions returns a unique source_id for a new source."""
        source_id = str(source_id)
        if source_id.startswith("|"):
            # Preventing collisions with special cases
            # Also preventing that weird forum post by someone who thought it was a good idea to use a single pipe as their source id
            source_id = source_id[1:0]
        if not id_list and hasattr(self, "sources"):
            id_list = self.get_values_from_sources("source_id")
        elif not id_list and not hasattr(self, "sources"):
            ValueError("Called before class was fully initialized without list of source_id's to compare it to")
            return
        if not source_id:
            source_id = "source"
        i = 0
        new_id = source_id
        while new_id in id_list:
            new_id = f"{source_id}{i}"
            i += 1
        return source_id

    def get_values_from_sources(self, key) -> list:
        """
        Returns a list of values for a given key from all sources.
        """
        _list = []
        with self._sources_lock:
            if self.sources:
                for source in self.sources:
                    if hasattr(source, key):
                        value = getattr(source, key)
                        if not callable(value):
                            _list.append(value)
        return _list

    def get_sources_by_value(self, key, value) -> list:
        """
        Returns list of SourceObjects with a matching value for a given key.
        """
        _list = []
        with self._sources_lock:
            if self.sources:
                for source in self.sources:
                    if not hasattr(source, key):
                        continue
                    attr_value = getattr(source, key)
                    if callable(attr_value):
                        continue
                    if attr_value == value:
                        _list.append(source)
        return _list

    def get_sources_by_class(self, sourceClass):
        """Returns list of sources of a given class."""
        with self._sources_lock:
            return [source for source in self.sources if isinstance(source, sourceClass)]

    def source_exists(self, SourceClass):
        """Returns True if a source of a given class exists."""
        with self._sources_lock:
            return any(isinstance(source, SourceClass) for source in self.sources)

    @property
    def get_sources_amount(self) -> int:
        """Returns the amount of sources. Includes user-defined source."""
        with self._sources_lock:
            return len(self.sources)

    @property
    def download_sources_available(self) -> bool:
        """Returns True if there is at least one download source available."""
        with self._sources_lock:
            _sources = self.get_sources_by_class(SourceDownload)
        return any(source.available for source in _sources)

    def select_source(self, source_id):
        """
        Change the selected source for this file.
        Returns the selected source (either class object or string if file is cached).
        """
        # Reset the override in case it was already set
        self.url_override(None)

        # If the source_id is already selected, do nothing
        if self.active_source is not None and self.active_source.source_id == source_id:
            return self.active_source

        # Special cases
        # Each of them must have a '|' as first character to avoid possible conflicts with scripts
        if source_id == "|init":
            if self.cache_source:
                return self.select_source(self.cache_source.source_id)
            # If there's just one it must be SourceUser, so let's check for it
            if self.get_sources_amount > 1:
                _sources = self.get_sources_by_class(SourceSteam)
                if len(_sources) > 0:
                    return self.select_source(_sources[0].source_id)
                _sources = self.get_sources_by_class(SourceDownload)
                if len(_sources) > 0:
                    # Pick the first source that isn't restricted, if possible
                    for _source in _sources:
                        if not _source.is_restricted:
                            self._first_valid_download_source = _source
                            return self.select_source(_source.source_id)
                    else:
                        self._first_valid_download_source = _sources[0]
                        return self.select_source(_sources[0].source_id)
                # Fallback in case something goes wrong
                return self.select_source(self._user_source.source_id)
            else:
                return self.select_source(self._user_source.source_id)
        elif source_id == "|auto_available":
            # Fallback in case something goes wrong during download, it will simply pick the first available downloadable source
            # Throws an error if no source is known to be available (the installer should never allow this to happen)
            _sources = self.get_sources_by_class(SourceDownload)
            for _source in _sources:
                if not _source.is_restricted:
                    return self.select_source(_source.source_id)
            if self._sources[0].available:
                return self.select_source(_sources[0].source_id)
            else:
                raise RuntimeError(
                    "No available URL found for %s, yet the download process was started. This should never happen."
                    % self.filename
                )
        else:
            if source_id not in self.get_values_from_sources("source_id"):
                logger.error("Invalid source id (%s), defaulting to first valid source" % source_id)
                return self.select_source("|init")

            # Do the actual selection
            self.active_source = self.get_sources_by_value("source_id", source_id)[0]
            logger.debug(
                "Download source for %s changed to %s (source_id: %s)",
                self.filename,
                self.active_source.domain or self.active_source.__class__.__name__,
                self.active_source.source_id,
            )
            return self.active_source

    def change_provider(self, new_provider):
        """Selects a source that accomodates the new provider. Expects the provider string."""
        if new_provider == "download":
            self.select_source(self._first_valid_download_source.source_id)
        elif new_provider == "steam":
            self.select_source(self.get_sources_by_class(SourceSteam)[0].source_id)
        elif new_provider == "user":
            self.select_source(self._user_source.source_id)
        elif new_provider == "pga":
            if self.cache_source:
                self.select_source(self.cache_source.source_id)
            else:
                raise ValueError("No cache source available for %s" % self.url)
        else:
            raise ValueError("Unsupported provider for %s: %s" % self.url, new_provider)

    ######################################################
    ################ Signal Handling & UI ################
    ######################################################

    def on_speed_measured(self, widget, url, speed):
        """Receives a speed measurement and updates known values"""
        _list = self.get_sources_by_class(SourceDownload)
        for source in _list:
            if source.domain == urlparse(url).netloc and not source.url == url:
                source.speed = speed

    def create_download_progress_box(self) -> DownloadProgressBox:
        return DownloadProgressBox(
            url=self.url, dest=self.dest_file, temp=self.download_file, referer=self.referer, downloader=self.downloader
        )

    @property
    def human_url(self) -> str:
        """Return the currently active source url in human-readable, UI-friendly format"""
        if isinstance(self.active_source, SourceUser):
            # Ask the user where the file is located
            parts = self.active_source.url.split(":", 1)
            if len(parts) == 2:
                return parts[1]
            return "Please select file '%s'" % self.id
        return self.active_source.url

    def get_label(self) -> str:
        """Return a human readable label for installer files"""
        if isinstance(self.active_source, SourceDownload):
            if not self.is_offline:
                label = __("{file} on {hosts} host", "{file} on {hosts} hosts", self.available_downloads).format(
                    file=self.filename, hosts=self.available_downloads
                )
            else:
                label = _("{file} unavailable. Choose another Source.").format(file=self.filename)
        else:
            label = self.filename
        return gtk_safe_urls(label)

    @property
    def auxiliary_info(self):
        """Provides a small bit of additional descriptive texts to show in the UI."""
        return None

    ######################################################
    ################# Caching & Download #################
    ######################################################

    def is_ready(self, provider) -> bool:
        """Is the file ready to be downloaded or already present at the destination (if applicable)?"""
        if provider == "download":
            return self.available_downloads > 0
        return provider not in ("user", "pga") or system.path_exists(self.dest_file)

    @property
    def is_user_pga_caching_allowed(self) -> bool:
        """Returns true if this file can be transferred to the cache, if
        the user provides it."""
        return self.uses_pga_cache()

    @property
    def is_cached(self) -> bool:
        """Is the file available in the user-specified PGA cache?
        This doesn't check for temporary cached files."""
        return self.uses_pga_cache() and system.path_exists(self.dest_file)

    @property
    def cache_source_available(self) -> bool:
        """Returns true of the a cache source is available, either temporary or PGA"""
        return isinstance(self.cache_source, SourceCache)

    @property
    def _cache_path(self) -> str:
        """Return the directory used as a cache for the duration of the installation"""
        return get_url_cache_path(self.url, self.id, self.game_slug)

    def save_to_cache(self):
        """Copy the file into the PGA cache."""

        cache_path = self._cache_path
        try:
            if not os.path.isdir(cache_path):
                logger.debug("Creating cache path %s", self._cache_path)
                os.makedirs(cache_path)
        except (OSError, PermissionError) as ex:
            logger.error("Failed to created cache path: %s", ex)
            return

        save_to_cache(self.dest_file, cache_path)

    def uses_pga_cache(self) -> bool:
        """Determines whether the installer files are stored in a PGA cache"""
        if self.has_no_sources:
            return False
        return has_valid_custom_cache_path()

    def check_hash(self):
        """Check the hash of the active file after download (if available)"""
        self.active_source.check_hash(self.dest_file)

    def remove_previous(self):
        """Remove file at already at destination, prior to starting the download."""
        if not self.uses_pga_cache() and system.path_exists(self.dest_file):
            # If we've previously downloaded a directory, we'll need to get rid of it
            # to download a file now. Since we are not using the cache, we don't keep
            # these files anyway - so it should be safe to just nuke and pave all this.
            if os.path.isdir(self.dest_file):
                system.delete_folder(self.dest_file)
            else:
                os.remove(self.dest_file)

    @property
    def cached_filename(self) -> str:
        """Return the filename of the first file in the cache path"""
        cache_files = os.listdir(self._cache_path)
        if cache_files:
            return cache_files[0]
        return ""

    @property
    def downloader(self) -> Optional[Downloader]:
        """Returns optional downloader class or value of currently active source. Feature used for itchio."""
        if hasattr(self.active_source, "downloader") and callable(self.active_source._downloader):
            return self.active_source.downloader

    def is_downloadable(self) -> bool:
        """Return True if the file can be downloaded (even from the local filesystem)"""
        return len(self.get_sources_by_class(SourceDownload)) > 0

    @property
    def dest_file(self) -> str:
        def find_dest_file():
            for alt_name in self.get_alternate_filenames():
                alt_path = os.path.join(self._cache_path, alt_name)
                if os.path.isfile(alt_path):
                    return alt_path
            return os.path.join(self._cache_path, self.filename)

        if self._dest_file_override:
            return self._dest_file_override
        if not self._dest_file_found:
            self._dest_file_found = find_dest_file()
        return self._dest_file_found

    @dest_file.setter
    def dest_file(self, value):
        self._dest_file_override = value

    @property
    def download_file(self) -> str:
        """This is the actual path to download to; this file is renamed to the
        dest_file when complete."""
        return self.dest_file + ".tmp"

    def override_dest_file(self, new_dest_file):
        """Called by the UI when the user selects a file path."""
        self._user_source.dest_file = new_dest_file

    @property
    def is_dest_file_overridden(self) -> bool:
        return bool(self._dest_file_override)

    def get_dest_files_by_id(self) -> Dict[str, str]:
        """Returns a dict of {installer_file_id: dest_file}"""
        return {self.id: self.dest_file}

    ######################################################
    ######################## Other #######################
    ######################################################

    def copy(self) -> "InstallerFile":
        """Copies this file object, so the copy can be modified safely."""
        file = InstallerFile(self.game_slug, self.id, self._file_meta)
        file._filename = self._filename
        file._url_override = self._url_override
        file._dest_file_override = self._dest_file_override
        file._dest_file_found = self._dest_file_found
        return file

    @property
    def url(self) -> str:
        """Returns the selected URL for this file"""
        if self._url_override:
            return self._url_override
        else:
            return self.active_source.url

    def url_all(self, remote_only=True) -> list:
        """Returns list of URLs file is hosted on"""
        _list = self.get_values_from_sources("url")
        if remote_only:
            return [url for url in _list if url.startswith("http")]
        else:
            return _list

    def url_override(self, url):
        """Modify the selected URL and set override"""
        if self._url_override not in (url, None):
            logger.debug("Overriding URL for %s: %s", self.id, url if url is not None else "(None)")
        self._url_override = url

    @property
    def speed(self) -> Optional[float]:
        """Returns the measured speed of the selected URL for this file"""
        if self.active_source.speed is False:
            return None
        else:
            return self.active_source.speed

    @property
    def speed_fastest(self) -> Optional[Tuple[str, float]]:
        """If available, returns the fastest measured download speed for this file with the associated URL as a tuple (url: speed)"""
        _list = self.get_values_from_sources("speed")
        _list = [
            speed for speed in _list if speed not in (False, None)
        ]  # 'False' indicate a failed test or not being applicable, not a measurement of 0.0
        if not _list:
            return
        sources = self.get_sources_by_value("speed", max(_list))
        return (sources[0].url, sources[0].speed)

    def run_speedtest(self, overwrite_existing=False):
        """Run a speedtest on all testable sources sequentially. Blocking call."""

        def _add_cached(memfile: BytesIO):
            # Called in the main thread if the file has been fully downloaded during the speedtest
            _new_source = SourceCache(
                self._unique_id("tempcache"),
                self.dest_file,
                memfile=memfile,
                tmpfile=self.download_file,
                temporary=True,
            )
            self.add_source(_new_source)
            self.cache_source = _new_source
            logger.debug("File fully downloaded during speedtest, added new cache source to %s", self.filename)

        schedule_at_idle(self.emit, "processing-start")
        _list = self.get_sources_by_class(SourceDownload)
        try:
            for source in _list:
                with check_stop(thread_namespace.stop_request):
                    if not overwrite_existing and source.speed:
                        continue
                    _temp = source.speed_test()
                    if isinstance(_temp, BytesIO):
                        if not self.cache_source:
                            schedule_at_idle(self.prepare)
                            schedule_at_idle(_add_cached, _temp)
                        break
        except StopRequested:
            return
        schedule_at_idle(self.emit, "processing-stop")

    def get_availability(self, all_files=True):
        """Run the availability check on all testable sources."""
        schedule_at_idle(self.emit, "processing-start")
        _list = self.get_sources_by_class(SourceDownload)
        try:
            for source in _list:
                with check_stop(thread_namespace.stop_request):
                    if not all_files and source.available:
                        continue
                    else:
                        source.get_availability()
        except StopRequested:
            return
        schedule_at_idle(self.emit, "processing-stop")

    @property
    def available_downloads(self) -> int:
        """Returns the number of confirmed available download sources."""
        return len(self.get_sources_by_value("available", True))

    @property
    def is_offline(self) -> bool:
        """Returns true if every downloadable source is confirmed offline."""
        # Allows us to make the distinction between confirmed available downloads and confirmed unavailable ones, as some sources may not be checked yet.
        sources = self.get_sources_by_class(SourceDownload)
        if not sources:
            return False
        for source in sources:
            if source.available is not False:
                return False
        return True

    @property
    def domains(self) -> list:
        """
        Returns a list of all domains the file is hosted on.
        This only includes downloadable sources.
        """
        _source_list = self.get_sources_by_class(SourceDownload)
        _list = []
        for source in _source_list:
            _list.append(source.domain)
        return _list

    @property
    def filename(self) -> str:
        if self.sources:
            if self._multiple_sources:
                if not self._filename:
                    raise ScriptingError(_("missing field `filename` in file `%s`") % self.id)
                return self._filename
            else:
                if len(self.sources) == 1:  # If there's only one source it has to be the user source ("N/A")
                    if self.uses_pga_cache() and os.path.isdir(self._cache_path):
                        return self.cached_filename
                    return ""
                external_source = next((source for source in self.sources if not isinstance(source, SourceUser)), None)
                if isinstance(external_source, SourceSteam):
                    return self.active_source.url
                return os.path.basename(external_source.url)

    @filename.setter
    def filename(self, name):
        self._filename = name

    def get_alternate_filenames(self, source_id=None) -> list[str]:
        """Returns a list of alternate filenames. Returns the active source values if none is given."""
        if not source_id:
            source_id = self.active_source.source_id
        source = self.get_sources_by_value("source_id", source_id)[0]
        if hasattr(source, "_alternate_filenames"):
            return source._alternate_filenames
        else:
            return []

    @property
    def referer(self) -> str:
        """Returns referer of currently active source"""
        return self.active_source.referer

    @property
    def default_provider(self) -> str:
        """Return file provider used by active source"""
        if isinstance(self.active_source, SourceCache):
            return "pga"
        if isinstance(self.active_source, SourceSteam):
            return "steam"
        if isinstance(self.active_source, SourceUser):
            return "user"
        if self.is_downloadable():
            return "download"
        raise ValueError("Unsupported provider for %s" % self.url)

    @property
    def has_no_sources(self) -> bool:
        """Returns true if the only source for this file is the user"""
        if hasattr(self, "sources") and len(self.sources) == 1 and isinstance(self.active_source, UserSource):
            return True
        return False

    @property
    def providers(self) -> Set[str]:
        """Return all supported providers"""
        _providers = set()
        if len(self.get_sources_by_class(SourceSteam)) > 0:
            _providers.add("steam")
        if isinstance(self.cache_source, SourceCache):
            _providers.add("pga")
        _providers.add("user")
        if self.is_downloadable():
            _providers.add("download")
        return _providers

    def prepare(self):
        """Prepare the file for download. If we've not been redirected to an existing file,
        anwe will create directories to contain the cached file."""
        if not self.is_dest_file_overridden:
            get_url_cache_path(self.url, self.id, self.game_slug, prepare=True)

    @property
    def size(self) -> Optional[int]:
        """Return size of active source as defined by file provider."""
        try:
            size = int(self.active_source.size)
            if size >= 0:
                return size
        except (ValueError, TypeError):
            return None
        return None

    @property
    def total_size(self) -> Optional[int]:
        """Return total size of active source as defined by file provider."""
        try:
            total_size = int(self.active_source.total_size)
            if total_size >= 0:
                return total_size
        except (ValueError, TypeError):
            return None
        return None

    def __str__(self):
        return "%s/%s" % (self.game_slug, self.id)
