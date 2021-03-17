import geocoder
from cachetools.func import lru_cache
from caching import hashable_cache


@hashable_cache(lru_cache())
def get_geocoder(location):
    return geocoder.google(location)
