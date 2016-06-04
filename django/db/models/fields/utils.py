# -*- coding: utf-8 -*-


class FieldCacheMixin(object):
    """
    Provides a common API for working with the fields
    value cache on an instance.
    """

    def get_cache_name(self):
        return self.name

    def get_cached_value(self, instance):
        cache_name = self.get_cache_name()
        return instance._state.get_fields_cache()[cache_name]

    def is_cached(self, instance):
        cache_name = self.get_cache_name()
        return cache_name in instance._state.get_fields_cache()

    def set_cached_value(self, instance, value):
        instance._state.set_fields_cache(field=self, value=value)

    def delete_cached_value(self, instance):
        cache_name = self.get_cache_name()
        del instance._state.cache[cache_name]
