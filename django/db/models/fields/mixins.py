import copy
from collections import defaultdict

NOT_PROVIDED = object()


class FieldCacheMixin:
    """Provide an API for working with the model's fields value cache."""

    def get_cache_name(self):
        raise NotImplementedError

    def get_cached_value(self, instance, default=NOT_PROVIDED):
        cache_name = self.get_cache_name()
        try:
            return instance._state.fields_cache[cache_name]
        except KeyError:
            # An ancestor link will exist if this field is defined on a
            # multi-table inheritance parent of the instance's class.
            ancestor_link = instance._meta.get_ancestor_link(self.model)
            if ancestor_link:
                try:
                    # The value might be cached on an ancestor if the instance
                    # originated from walking down the inheritance chain.
                    ancestor = ancestor_link.get_cached_value(instance)
                except KeyError:
                    pass
                else:
                    value = self.get_cached_value(ancestor)
                    # Cache the ancestor value locally to speed up future
                    # lookups.
                    self.set_cached_value(instance, value)
                    return value
            if default is NOT_PROVIDED:
                raise
            return default

    def is_cached(self, instance):
        return self.get_cache_name() in instance._state.fields_cache

    def set_cached_value(self, instance, value):
        instance._state.fields_cache[self.get_cache_name()] = value

    def delete_cached_value(self, instance):
        del instance._state.fields_cache[self.get_cache_name()]


class PrefetchMixin(FieldCacheMixin):

    def get_prefetch_remote_key(self, obj):
        """
        Returns an identifier for the instance referenced
        by this object given an instance of the referenced object.
        """
        # In a ManyToOne relationship, obj is an instance
        # of the object referenced by the ForeignKey field and this
        # should return its id.
        raise NotImplementedError

    def get_prefetch_local_key(self, obj):
        """
        Returns an identifier for the instance referenced
        by this object given an instance of the object with the reference.
        """
        # In a ManyToOne relationship, obj is an instance
        # of the object with the ForeignKey field and this
        # should return the id of the object referenced.
        raise NotImplementedError

    def get_prefetch_objects(self, instances, queryset=None):
        raise NotImplementedError

    def set_prefetch_value(self, obj, value, through_attr, lookup, level):
        raise NotImplementedError

    def get_related_objects_cache(self, related_objects):
        raise NotImplementedError

    def prefetch_objects(self, lookup, instances, level, to_attr=None):
        """
        Run prefetches on all instances,
        and assigns results to relevant caches for each instance.

        Return the prefetched objects along with any additional prefetches that
        must be done due to prefetch_related lookups found from default managers.
        """
        lookup_queryset = lookup.get_current_queryset(level)
        related_objects = self.get_prefetch_objects(instances, lookup_queryset)
        instance_attr = self.get_prefetch_local_key

        # The QuerySet that just came back might contain some
        # prefetch_related lookups. We don't want to trigger the
        # prefetch_related functionality when the queryset is evaluated below.
        # Rather, we need to merge in the prefetch_related lookups.
        if getattr(related_objects, '_prefetch_done', False):
            executed_prefetch_lookups = [
                copy.copy(inherited_lookup) for inherited_lookup
                in getattr(related_objects, '_prefetch_related_lookups', ())
            ]
            delayed_prefetch_lookups = []
        else:
            executed_prefetch_lookups = []
            # Delayed lookups are found when nesting prefetch_related() calls.
            # They're delayed because they're executed after the current
            # prefetch operation call finishes.
            delayed_prefetch_lookups = [
                copy.copy(inherited_lookup) for inherited_lookup
                in getattr(related_objects, '_prefetch_related_lookups', ())
            ]

        if (executed_prefetch_lookups or delayed_prefetch_lookups):
            # Don't need to clone because the manager should have given us a fresh
            # instance, so we access an internal instead of using public interface
            # for performance reasons.
            related_objects._prefetch_related_lookups = ()

        # Because related_objects can be a Queryset() instance
        # evaluate and cast it to a list.
        all_related_objects = list(related_objects)

        rel_obj_cache = self.get_related_objects_cache(all_related_objects)

        if to_attr:
            for obj in instances:
                instance_attr_val = instance_attr(obj)
                value = rel_obj_cache[instance_attr_val]
                setattr(obj, to_attr, value)
        else:
            through_attr = lookup.get_current_prefetch_to(level)

            for obj in instances:
                instance_attr_val = instance_attr(obj)
                self.set_prefetch_value(
                    obj,
                    value=rel_obj_cache[instance_attr_val],
                    through_attr=through_attr,
                    lookup=lookup,
                    level=level,
                )
        return all_related_objects, delayed_prefetch_lookups, executed_prefetch_lookups


class PrefetchSingleValueMixin(PrefetchMixin):

    def set_prefetch_value(self, obj, value, through_attr, lookup, level):
        self.set_cached_value(obj, value)

    def get_related_objects_cache(self, related_objects):
        rel_obj_cache = defaultdict(lambda: None)

        for rel_obj in related_objects:
            rel_attr_val = self.get_prefetch_remote_key(rel_obj)
            rel_obj_cache[rel_attr_val] = rel_obj
        return rel_obj_cache


class PrefetchMultiValueMixin(PrefetchMixin):

    multi_value = True

    def set_prefetch_value(self, obj, value, through_attr, lookup, level):
        manager = getattr(obj, through_attr)

        # This can be None if this is not the last level on the prefetch path
        lookup_queryset = lookup.get_current_queryset(level)

        if lookup_queryset is not None:
            # Filter the queryset for the instance this manager is bound to.
            qs = manager._apply_rel_filters(lookup_queryset)
        else:
            qs = manager.get_queryset()

        qs._result_cache = value

        # All prefetches defined in the lookup queryset
        # were merged into the current prefetch operation,
        # as a result, mark the lookup queryset as prefetched
        # to prevent it from executing them again.
        qs._prefetch_done = True

        self.set_cached_value(obj, qs)

    def get_related_objects_cache(self, related_objects):
        rel_obj_cache = defaultdict(list)

        for rel_obj in related_objects:
            rel_attr_val = self.get_prefetch_remote_key(rel_obj)
            rel_obj_cache[rel_attr_val].append(rel_obj)
        return rel_obj_cache
