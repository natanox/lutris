"""Widget to select the source of a file from multiple URLs"""

from gettext import gettext as _
from urllib.parse import urlparse

from gi.repository import GObject, Gtk, GdkPixbuf, Gdk, GLib

from lutris.installer.installer_file_source import SourceDownload
from lutris.util.log import logger


class UrlPicker(Gtk.ComboBox):
    """Widget to select the source of a file from multiple URLs"""

    __gsignals__ = {
        "update-label": (GObject.SIGNAL_RUN_FIRST, None, ()),
        "file-ready": (GObject.SIGNAL_RUN_FIRST, None, ()),
        "file-unready": (GObject.SIGNAL_RUN_FIRST, None, ()),
    }

    def __init__(self, installer_file):
        super().__init__()

        #
        # Style related things
        #
        self.textcolor_normal = self.get_style_context().get_color(Gtk.StateFlags.NORMAL).to_string()
        self.textcolor_unavailable = self.get_style_context().get_color(Gtk.StateFlags.INSENSITIVE).to_string()
        if (
            self.textcolor_normal == self.textcolor_unavailable
        ):  # Fallback for themes that don't have a different color for insensitive items
            rgba = Gdk.RGBA()
            rgba.parse(self.textcolor_normal)
            rgba.alpha = 0.4
            self.textcolor_unavailable = rgba.to_string()
        # This is a little hack to change the color of the label when the file is not available while making sure it works with every theme
        # Unfortunately Pango markup does not support rgba colors yet so we need to use this workaround for now
        # self.css_provider_labels = Gtk.CssProvider()
        # css = f"""
        # .label-unavailable {{
        #    color: {self.textcolor_unavailable};
        # }}
        # """
        # self.css_provider_labels.load_from_data(css.encode("utf-8"))
        # Gtk.StyleContext.add_provider_for_screen(self.get_screen(), self.css_provider_labels, Gtk.STYLE_PROVIDER_PRIORITY_APPLICATION)
        # TODO: Check if this was still relevant somewhere

        #
        # Initialization
        #
        self.installer_file = installer_file
        self.url_picker_store = Gtk.ListStore(
            str, str, str, GdkPixbuf.Pixbuf, str
        )  # url, domain, speed, reachability icon, foreground colour (text)
        style_context = self.get_style_context()
        self.urls = []  # List of urls for dunder/special methods so we can access them without GLib.idle_add
        # In case someone clicks an unavailable source we want it to look like nothing happened.
        # GTK doesn't know the concept if making a row unavailable, however it's advantageous for script maintainers to receive feedback if files go offline.
        self.last_valid_source_index = 0

        #
        # Populate the widget
        #
        if self.installer_file._multiple_sources:
            safe_label = GLib.markup_escape_text(_("Auto (fastest)"))
            self.url_picker_store.append(["|auto_fastest", _("Auto (fastest)"), "", None, self.textcolor_normal])
        for source in self.installer_file.get_sources_by_class(SourceDownload):
            pixbuf = source.available_icon
            self.url_picker_store.append(
                [
                    source.url,
                    source.domain,
                    source.speed_human_readable(),
                    pixbuf,
                    self.textcolor_normal
                    if source.available and self.installer_file._multiple_sources
                    else self.textcolor_unavailable,
                ]
            )
            self.urls.append(source.url)
        self.set_model(self.url_picker_store)
        if not self.installer_file._multiple_sources:
            self.get_style_context().add_class("remove-arrow")
            self.set_sensitive(False)

        # We're setting the box to default to the first entry (likely |auto_fastest), however installer_file will instead default on the first viable url.
        # This is intentional behaviour so we never get stuck in some invalid state and avoid abstractions in all the wrong places.
        self.set_id_column(0)

        # Column showing Domain names
        renderer_text_left = Gtk.CellRendererText()
        self.pack_start(renderer_text_left, True)
        self.add_attribute(renderer_text_left, "text", 1)
        self.add_attribute(renderer_text_left, "foreground", 4)

        # Column showing speed, if available
        renderer_text_right = Gtk.CellRendererText()
        renderer_text_right.set_property("xalign", 1.0)
        self.pack_start(renderer_text_right, True)
        self.add_attribute(renderer_text_right, "text", 2)
        self.add_attribute(renderer_text_right, "foreground", 4)

        # Column showing the availability icons
        icon_renderer = Gtk.CellRendererPixbuf()
        icon_renderer.set_property("xpad", 5)
        self.pack_start(icon_renderer, False)
        self.add_attribute(icon_renderer, "pixbuf", 3)
        self.connect("changed", self.on_download_source_changed)

        self.set_active(0)
        self.show()

        # Lastly, connect the appropriate signals
        self.installer_file.connect("federate-speed-measured", self.on_speed_measured)
        for source in self.installer_file.get_sources_by_class(SourceDownload):
            source.connect("new-availability-measured", self.on_availability_changed)

    def on_download_source_changed(self, widget):
        """Change the source of the file to download according to user selection"""
        tree_iter = self.get_active_iter()
        url = self.url_picker_store[tree_iter][0]
        if not url == "|auto_fastest":
            source = self.installer_file.get_sources_by_value("url", url)[0]
            if source.available:
                self.installer_file.select_source(source.source_id)
                self.last_valid_source_index = widget.get_active()
            else:
                self.set_active(self.last_valid_source_index)
        # |auto_fastest will be resolved at start of installation

    def change_download_source(self, url):
        """Change the active url picker combobox item and source of the file to download programmatically"""
        iter = self.url_picker_store.get_iter_first()
        while iter is not None:
            row_url = self.url_picker_store.get_value(iter, 0)
            if row_url == url:
                self.set_active_iter(iter)
                break
            iter = self.url_picker_store.iter_next(iter)
        else:
            logger.error("change_download_source called with invalid url: %s", url)

    def on_speed_measured(self, widget, url, speed):
        """Updates known speed values for all URLs pointing to the same domain and updates the UI"""

        iter = self.url_picker_store.get_iter_first()
        while iter is not None:
            _url = self.url_picker_store.get_value(iter, 0)
            if urlparse(_url).netloc == urlparse(url).netloc:
                source = self.installer_file.get_sources_by_value("url", _url)[0]
                if not source.error and source.available:
                    self.url_picker_store.set_value(iter, 2, source.speed_human_readable())
                elif not source.available:
                    self.url_picker_store.set_value(iter, 2, "(Offline)")
                elif source.error:
                    self.url_picker_store.set_value(iter, 2, _("Error: {}").format(source.error_code))
                break
            iter = self.url_picker_store.iter_next(iter)

    def on_availability_changed(self, widget, url):
        """Called every time a new availability measurement is done by any downloadable source"""
        iter_ = self.url_picker_store.get_iter_first()
        while iter_ is not None:
            row_url = self.url_picker_store.get_value(iter_, 0)
            if row_url == url:
                new_pixbuf = widget.available_icon
                self.url_picker_store.set_value(iter_, 3, new_pixbuf)
                self.url_picker_store.set_value(
                    iter_, 4, self.textcolor_normal if widget.available else self.textcolor_unavailable
                )
                self.emit("update-label")
                if self.installer_file.is_offline:
                    self.emit("file-unready")
                    self.set_sensitive(False)
                    self.get_style_context().add_class("remove-arrow")
                    self.url_picker_store.set_value(
                        self.url_picker_store.get_iter_first(), 4, self.textcolor_unavailable
                    )
                elif isinstance(self.installer_file.active_source, SourceDownload) and not self.installer_file.is_ready(
                    "download"
                ):
                    self.emit("file-unready")
                elif isinstance(self.installer_file.active_source, SourceDownload) and self.installer_file.is_ready(
                    "download"
                ):
                    self.emit("file-ready")
                break
            iter_ = self.url_picker_store.iter_next(iter_)

    @property
    def selected_url(self):
        """Returns the selected URL (or |auto_fastest), if applicable"""
        return self.get_active_id()

    def __len__(self):
        """Returns the amount of URLs in the picker."""
        return len(self.urls)

    def __str__(self):
        """Returns class name and list of domains in the picker."""
        return f"{self.__class__.__name__}({[urlparse(url).netloc for url in self.urls]})"

    def __contains__(self, url):
        """Returns True if a url or domain with the given name is in the picker."""
        for u in self.urls:
            if u == url or urlparse(u).netloc == urlparse(url).netloc:
                return True
        return False
