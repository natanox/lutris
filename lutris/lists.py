"""Contains lists used for various purposes as well as helper functions for them."""

from urllib.parse import urlparse


def restricted_domains() -> list:
    """Returns a list of domains Lutris should avoid to download from if there's a choice."""
    # If you add new entries please keep it alphabetically sorted and add a comment about why it's' restricted.

    return [
        # Archive.org
        # Given their importance to the preservation of software it should be a source of last resort se we don't
        # unnecessarily induce more traffic to their servers.
        "archive.org"
    ]


def restricted_domains_contain(url) -> bool:
    """Returns true if the domain is restricted. Also accepts whole URLs."""

    if not url.startswith("http"):
        return False

    domain = urlparse(url).netloc
    _domains = restricted_domains()
    return any(domain.endswith(restricted_domain) for restricted_domain in _domains)
