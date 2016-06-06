# -*- coding: utf-8 -*-

NOT_PROVIDED = object()


class FieldCacheMixin(object):
    """
    Provides a common API for working with the fields
    value cache on an instance.
    """

    def get_cache_name(self):
        return self.name

    def get_cached_value(self, instance, default=NOT_PROVIDED):
        cache_name = self.get_cache_name()

        try:
            return instance._state.cache[cache_name]
        except KeyError:
            if default is NOT_PROVIDED:
                raise
            return default

    def is_cached(self, instance):
        cache_name = self.get_cache_name()
        return cache_name in instance._state.cache

    def set_cached_value(self, instance, value):
        instance._state.cache[self.get_cache_name()] = value

    def delete_cached_value(self, instance):
        cache_name = self.get_cache_name()
        del instance._state.cache[cache_name]
