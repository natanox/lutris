"""Representations of installer file sources"""

import os
import time
from io import BytesIO
from gettext import gettext as _
from typing import Optional, Union
from urllib.parse import urlparse

from gi.repository import GObject, Gtk, GdkPixbuf, GLib, Gdk

from lutris.util import system
from lutris.util.downloader import Downloader, is_file_available
from lutris.util.log import logger
from lutris.lists import restricted_domains_contain
from lutris.util.jobs import thread_namespace, check_stop, StopRequested, schedule_at_idle


class InstallerFileSource(GObject.Object):
    """
    Representation of a single source for a file in the `files` sections of an installer.
    This class is never instanced directly. Use one of the 'SourceXY' subclasses instead.
    """

    # Any functionality shared by every source but not related to the installer file or how Lutris uses it should go here

    def __init__(self, source_id, url, **kwargs):
        super().__init__()
        self.source_id = str(source_id)
        self._url = url
        self.checksum = kwargs.get("checksum")
        self.size = kwargs.get("size")
        self.total_size = kwargs.get("total_size")

    @property
    def domain(self):
        """stub. Returns None."""
        return None

    @property
    def available(self):
        """stub. Returns None."""
        return None

    @property
    def available_icon(self):
        """stub. Returns None."""
        return None

    @property
    def url(self):
        """Returns url of source."""
        return self._url

    @url.setter
    def url(self, url):
        """Sets url of source."""
        self._url = url

    @property
    def speed(self):
        """stub. Returns False."""
        return False

    def __str__(self):
        return "%s: %s" % (self.__class__.__name__, self.url)

    def check_hash(self, dest_file):
        """Checks the checksum of `file` and compare it to `value`

        Args:
            checksum (str): The checksum to look for (type:hash)
            dest_file (str): The path to the destination files
            dest_file_uri (str): The uri for the destination file
        """
        if not self.checksum or not dest_file:
            return
        try:
            hash_type, expected_hash = self.checksum.split(":", 1)
        except ValueError as err:
            raise ScriptingError(_("Invalid checksum, expected format (type:hash) "), self.checksum) from err

        logger.info("Checking hash %s for %s", hash_type, dest_file)
        calculated_hash = system.get_file_checksum(dest_file, hash_type)
        if calculated_hash != expected_hash:
            raise ScriptingError(
                hash_type.capitalize() + _(" checksum mismatch "), f"{expected_hash} != {calculated_hash}"
            )


class SourceSteam(InstallerFileSource):
    """A single Steam source for a file in the `files` sections of an installer"""

    def __init__(self, source_id, url, **kwargs):
        super().__init__(source_id, url, **kwargs)

    @property
    def path():
        """Returns filesystem path for this file"""
        parts = self._url.split(":")
        return parts[2]

    @property
    def appid():
        """Returns this sources' Steam appid"""
        parts = self._url.split(":")
        return parts[1]


class SourceDownload(InstallerFileSource):
    """A single download source for a file in the `files` sections of an installer"""

    __gsignals__ = {
        "new-speed-measured": (
            GObject.SIGNAL_RUN_LAST,
            None,
            (str, float),
        ),  # (url, speed) Measured and emitted by this class
        "new-availability-measured": (
            GObject.SIGNAL_RUN_LAST,
            None,
            (str,),
        ),  # (url) Measured and emitted by this class
    }

    def __init__(self, source_id, url, **kwargs):
        super().__init__(source_id, url, **kwargs)
        self.referer = kwargs.get("referer")
        self._available = None  # True if the remote source is available, call get_availability() to set this
        self._speed = None  # Measured speed in Bytes/s
        self._downloader = None  # Feature needed for itchio, see https://github.com/lutris/lutris/commit/178e894d2610b08cd287a2de20ad20a3757cbe55
        self.alternate_filenames = kwargs.get("alternate_filenames") or []
        self.error = None
        self.error_code = None

    @property
    def domain(self) -> str:
        """Returns the domain the file is hosted on"""
        return urlparse(self._url).netloc

    @property
    def available(self) -> bool:
        """Returns true if the remote source is available."""
        return self._available

    @available.setter
    def available(self, value: bool):
        self._available = value

    @property
    def available_icon(self) -> GdkPixbuf.Pixbuf:
        """Returns the GTK object with the icon reflecting current reachability."""
        icon_size_in_px = 16
        icon_theme = Gtk.IconTheme.get_default()
        try:
            if self._available is None:
                icon = icon_theme.load_icon("network-wireless-acquiring-symbolic", icon_size_in_px, 0)
            elif self._available == -1:
                icon = icon_theme.load_icon("network-no-route-symbolic", icon_size_in_px, 0)
            elif self._available is True:
                icon = icon_theme.load_icon("network-wireless-signal-excellent-symbolic", icon_size_in_px, 0)
            elif self._available is False:
                icon = icon_theme.load_icon("network-wireless-disconnected-symbolic", icon_size_in_px, 0)
            return icon
        except GLib.Error:
            logger.debug("Availability icon not found in theme")
            return None

    @property
    def speed(self) -> Optional[float]:
        """Returns the measured speed in Bytes/s"""
        return self._speed

    @speed.setter
    def speed(self, value: float):
        self._speed = value

    @property
    def downloader(self) -> Optional[Downloader]:
        # Special feature for itchio, see https://github.com/lutris/lutris/commit/178e894d2610b08cd287a2de20ad20a3757cbe55
        if callable(self._downloader):
            self._downloader = self._downloader(self)
        return self._downloader

    @property
    def is_restricted(self) -> bool:
        """Returns true if this source is on the restriction list.
        This means it won't be automatically picked for download unless it's the only option."""
        return restricted_domains_contain(self._url)

    def get_availability(self) -> bool:
        """Returns true if the remote source is available. Blocking call."""

        def _change(availability: bool):
            self.available = availability
            schedule_at_idle(self.emit, "new-availability-measured", self._url)

        if self.url.startswith("file://"):
            return os.path.exists(self.url[7:])
        _available = is_file_available(self.url, self.referer)
        schedule_at_idle(_change, _available)
        return _available

    def speed_human_readable(self, speed: float = None) -> str:
        """
        Return speed as a string in a more human-readable format.
        Returns default URL's speed if non is defined. Also define speed if you just want the appropriate string no matter if the URL actually is part of this file.
        """
        if not speed:
            if self.speed:
                speed = self.speed
            else:
                return None
        if speed < 1e3:
            return f"{speed:.1f} B/s"
        elif speed < 1e6:
            return f"{speed / 1e3:.1f} KB/s"
        elif speed < 1e9:
            return f"{speed / 1e6:.1f} MB/s"
        elif speed < 1e12:
            return f"{speed / 1e9:.1f} GB/s"
        elif speed < 1e15:
            return f"{speed / 1e12:.1f} TB/s"
        elif speed < 1e18:
            return f"{speed / 1e15:.1f} PB/s"
        elif speed < 1e21:
            return f"{speed / 1e18:.1f} EB/s"
        elif speed < 1e24:
            return f"{speed / 1e21:.1f} ZB/s"
        else:
            # Just in case
            return f"{speed / 1e24:.1f} YB/s"

    def speed_test(self) -> Optional[BytesIO]:
        """Run a speedtest on the URL. Blocking call."""
        _speedtest = Downloader(url=self.url, referer=self.referer, speedtest=True)
        _speedtest.start()
        _timeout = time.time() + 2.5
        _tmp = None

        def _success(speed):
            # Any value changes are deferred to the main thread (done here) for thread safety
            self.speed = speed
            self.emit("new-speed-measured", self.url, speed)

        def _error(speed, error, error_code):
            # Any value changes are deferred to the main thread (done here) for thread safety
            self.speed = False
            if self.available:
                logger.debug("Url was supposed to be available, yet the speedtest failed: %s", self.url)
                self.available = False
            self.error = error
            self.error_code = error_code
            self.emit("new-speed-measured", self.url, speed)

        try:
            while time.time() < _timeout:
                with check_stop(thread_namespace.stop_request):
                    if _speedtest.state == _speedtest.COMPLETED:
                        schedule_at_idle(_success, _speedtest.average_speed)
                        if _speedtest.is_file_completed:
                            _tmp = _speedtest.memfile
                        del _speedtest
                        return _tmp
                    elif _speedtest.state in (_speedtest.CANCELLED, _speedtest.ERROR):
                        schedule_at_idle(_error, _speedtest.average_speed, _speedtest.error, _speedtest.error_code)
                        del _speedtest
                        return
                time.sleep(0.1)
            else:
                logger.info("Speedtest timed out for %s", self.url)
        except StopRequested:
            _speedtest.cancel()


class SourceUser(InstallerFileSource):
    """A single user-provided source for a file in the `files` sections of an installer"""

    def __init__(self, source_id, url, **kwargs):
        super().__init__(source_id, url, **kwargs)

    @property
    def available(self) -> bool:
        """Returns true if the selected file is available."""
        return os.path.exists(self.url[7:])

    @property
    def url(self) -> str:
        """Returns currently selected path."""
        return self._url

    @url.setter
    def url(self, url):
        if url.startswith("file://"):
            self._url = url
        else:
            self._url = "file://" + url

    @property
    def available_icon(self) -> GdkPixbuf.Pixbuf:
        """Returns the GTK object with the icon reflecting file availability."""
        icon_size_in_px = 16
        icon_theme = Gtk.IconTheme.get_default()
        try:
            if self._available is True:
                icon = icon_theme.load_icon("gtk-ok-symbolic", icon_size_in_px, 0)
            elif self._available is False:
                icon = icon_theme.load_icon("remove-symbolic", icon_size_in_px, 0)
            return icon
        except GLib.Error:
            logger.debug("Availability icon not found in theme")
            return None


class SourceCache(InstallerFileSource):
    """A single cache source for a file in the `files` sections of an installer"""

    def __init__(self, source_id, url, **kwargs):
        if kwargs.get("memfile", False):
            self.save_memfile(kwargs.get("memfile"), kwargs.get("tmpfile"))
            os.rename(kwargs.get("tmpfile"), url)
        super().__init__(source_id, url, **kwargs)
        self._temporary = kwargs.get("temporary", False)  # True if this isn't from the users PGA cache

    @property
    def available(self) -> bool:
        """Returns true if the file is available in cache."""
        # If this ever returns false something is seriously wrong
        return os.path.exists(self.url[7:])

    @property
    def is_pga_cache(self) -> bool:
        """Returns true if this is a PGA cache source, not the temporary."""
        return not self._temporary

    @property
    def url(self) -> str:
        """Returns file path."""
        return self._url

    @url.setter
    def url(self, url):
        if url.startswith("file://"):
            self._url = url
        else:
            self._url = "file://" + url

    def save_memfile(self, memfile: BytesIO, destination: str) -> Union[Union[bool, str], Exception]:
        """Saves a file that was previously fully cached in memory during a speedtest to disk.
        Returns False if there is no cached file, otherwise returns destination."""
        try:
            with open(destination, "wb") as file_pointer:
                file_pointer.write(memfile.getvalue())
            file_pointer.close()
            memfile.close()
            del memfile
            return destination
        except (Exception, PermissionError) as e:
            logger.error("Failed to save cached memory file to disk: %s", e)
            return e
