from django.core.exceptions import ObjectDoesNotExist
from google.appengine.ext import ndb
from google.appengine.ext.ndb import (StringProperty, TextProperty, KeyProperty,
                                      BooleanProperty)
from tastypie import fields
from tastypie.bundle import Bundle
from tastypie.resources import Resource, ModelDeclarativeMetaclass


class ToOneNDBKeyField(fields.RelatedField):
    """
    Provides access to related data via NDB key.
    """
    help_text = 'A single related resource. Can be either a URI or set of nested resource data.'

    def __init__(self, to, attribute, related_name=None, default=fields.NOT_PROVIDED,
                 null=False, blank=False, readonly=False, full=False,
                 unique=False, help_text=None, use_in='all', full_list=True, full_detail=True):
        super(ToOneNDBKeyField, self).__init__(
            to, attribute, related_name=related_name, default=default,
            null=null, blank=blank, readonly=readonly, full=full,
            unique=unique, help_text=help_text, use_in=use_in,
            full_list=full_list, full_detail=full_detail
        )
        self.fk_resource = None

    def dehydrate(self, bundle):
        foreign_obj = None

        if isinstance(self.attribute, basestring):
            foreign_obj = bundle.obj
            try:
                foreign_obj = getattr(foreign_obj, self.attribute, None)
            except ObjectDoesNotExist:
                foreign_obj = None
        elif callable(self.attribute):
            foreign_obj = self.attribute(bundle)
        foreign_obj = foreign_obj.get()

        if not foreign_obj:
            if not self.null:
                raise fields.ApiFieldError("The model '%r' has an empty attribute '%s' and doesn't allow a null value." % (foreign_obj, self.attribute))
            return None

        self.fk_resource = self.get_related_resource(foreign_obj)
        fk_bundle = Bundle(obj=foreign_obj, request=bundle.request)
        return self.dehydrate_related(fk_bundle, self.fk_resource)

    def hydrate(self, bundle):
        value = super(ToOneNDBKeyField, self).hydrate(bundle)

        if value is None:
            return value

        new_bundle = self.build_related_resource(value, request=bundle.request)
        new_bundle.obj = new_bundle.obj.key
        return new_bundle


class ToManyNDBKeyField(fields.RelatedField):
    """
    This class is UNTESTED!
    """
    help_text = 'Many related resources. Can be either a list of URIs or list of individually nested resource data.'

    def __init__(self, to, attribute, related_name=None, default=fields.NOT_PROVIDED,
                 null=False, blank=False, readonly=False, full=False,
                 unique=False, help_text=None, use_in='all', full_list=True, full_detail=True):
        super(ToManyNDBKeyField, self).__init__(
            to, attribute, related_name=related_name, default=default,
            null=null, blank=blank, readonly=readonly, full=full,
            unique=unique, help_text=help_text, use_in=use_in,
            full_list=full_list, full_detail=full_detail
        )

    def dehydrate(self, bundle):
        if not bundle.obj or not bundle.obj.pk:
            if not self.null:
                raise fields.ApiFieldError("The model '%r' does not have a primary key and can not be used in a ToMany context." % bundle.obj)
            return []

        the_m2ms = None

        if isinstance(self.attribute, basestring):
            the_m2ms = bundle.obj
            try:
                the_m2ms = getattr(the_m2ms, self.attribute, None)
            except ObjectDoesNotExist:
                the_m2ms = None

        elif callable(self.attribute):
            the_m2ms = self.attribute(bundle)

        if not the_m2ms:
            if not self.null:
                raise fields.ApiFieldError("The model '%r' has an empty attribute '%s' and doesn't allow a null value." % (the_m2ms, self.attribute))

            return []

        self.m2m_resources = []
        m2m_dehydrated = []

        for m2m in the_m2ms.fetch(1000):
            m2m_resource = self.get_related_resource(m2m)
            m2m_bundle = Bundle(obj=m2m, request=bundle.request)
            self.m2m_resources.append(m2m_resource)
            m2m_dehydrated.append(self.dehydrate_related(m2m_bundle, m2m_resource))

        return m2m_dehydrated

    def hydrate(self, bundle):
        pass

    def hydrate_m2m(self, bundle):
        if self.readonly:
            return None

        if bundle.data.get(self.instance_name) is None:
            if self.blank:
                return []
            elif self.null:
                return []
            else:
                raise fields.ApiFieldError("The '%s' field has no data and doesn't allow a null value." % self.instance_name)

        m2m_hydrated = []

        for value in bundle.data.get(self.instance_name):
            if value is None:
                continue

            kwargs = {
                'request': bundle.request,
                }

            if self.related_name:
                kwargs['related_obj'] = bundle.obj
                kwargs['related_name'] = self.related_name

            new_bundle = self.build_related_resource(value, **kwargs)
            new_bundle.obj = new_bundle.obj.key
            m2m_hydrated.append(new_bundle)

        return m2m_hydrated


class NDBResource(Resource):
    """
    A very basic Tastypie Resource class for NDB models.

    NB: *Good* filtering and some NDB model properties are NOT implemented.
    """

    __metaclass__ = ModelDeclarativeMetaclass

    @classmethod
    def api_field_from_model_field(cls, f, default=fields.CharField):
        """
        Returns the field type that would likely be associated with each
        NDB model type.
        """
        result = default
        internal_type = type(f).__name__

        if internal_type in ('DateProperty', 'DateTimeProperty'):
            result = fields.DateTimeField
        elif internal_type in ('BooleanProperty',):
            result = fields.BooleanField
        elif internal_type in ('FloatProperty',):
            result = fields.FloatField
        elif internal_type in ('IntegerProperty',):
            result = fields.IntegerField
        elif internal_type in ('TimeProperty',):
            result = fields.TimeField

        return result

    @classmethod
    def get_fields(cls, fields=None, excludes=None):
        """
        Given any explicit fields to include and fields to exclude, add
        additional fields based on the associated model.
        """
        final_fields = {}
        fields = fields or []
        excludes = excludes or []

        if not cls._meta.object_class:
            return final_fields

        for f in cls._meta.object_class._properties.values():
            # If the field name is already present, skip
            setattr(f, 'name', f._name)
            if f.name in cls.base_fields:
                continue

            # If field is not present in explicit field listing, skip
            if fields and f.name not in fields:
                continue

            # If field is in exclude list, skip
            if excludes and f.name in excludes:
                continue

            api_field_class = cls.api_field_from_model_field(f)

            kwargs = {
                'attribute': f.name,
                'unique': False,
            }

            if not f._required:
                kwargs['null'] = True
                kwargs['default'] = ''
                kwargs['blank'] = True

            if isinstance(f, StringProperty) or isinstance(f, TextProperty):
                kwargs['default'] = ''

            if isinstance(f, KeyProperty):
                kwargs['full'] = True

            if f._default:
                kwargs['default'] = f._default

            if getattr(f, 'auto_now', False):
                kwargs['default'] = f.auto_now

            if getattr(f, 'auto_now_add', False):
                kwargs['default'] = f.auto_now_add

            final_fields[f.name] = api_field_class(**kwargs)
            final_fields[f.name].instance_name = f.name

        return final_fields

    # Below are the functions that are required to be implemented for Tastypie
    def detail_uri_kwargs(self, bundle_or_obj):
        kwargs = {}

        if isinstance(bundle_or_obj, Bundle):
            kwargs['pk'] = bundle_or_obj.obj.key.urlsafe()
        else:
            kwargs['pk'] = bundle_or_obj.key.urlsafe()

        return kwargs

    def get_object_list(self, request):
        return self._meta.object_class.query()

    def obj_get_list(self, request=None, **kwargs):
        filters = {}
        bundle = kwargs.get('bundle')
        if hasattr(bundle, 'request') and hasattr(bundle.request, 'GET'):
            filters = bundle.request.GET.copy()

        object_list = self.get_object_list(request)
        filtered_list = self.apply_filters(object_list, filters)
        return list(filtered_list.fetch(self._meta.max_limit))

    def obj_get(self, request=None, **kwargs):
        try:
            obj_key = ndb.Key(urlsafe=kwargs.get('pk'))
            return obj_key.get()
        except Exception:
            raise ObjectDoesNotExist("Couldn't find an instance of %s" % kwargs.get('pk'))

    def obj_create(self, bundle, request=None, **kwargs):
        bundle = self.full_hydrate(bundle)
        if 'pk' in kwargs:
            try:
                bundle.obj.key = ndb.Key(urlsafe=kwargs.get('pk'))
            except Exception:
                raise ObjectDoesNotExist("Couldn't find an instance of %s" % kwargs.get('pk'))
        bundle.obj.put()
        return bundle

    def obj_update(self, bundle, request=None, **kwargs):
        return self.obj_create(bundle, request, **kwargs)

    def obj_delete_list(self, request=None, **kwargs):
        obj_list = self.obj_get_list(request, kwargs)
        ndb.delete_multi([obj.key for obj in obj_list])

    def obj_delete(self, request=None, **kwargs):
        obj = self.obj_get(kwargs)
        obj.delete()

    def rollback(self, bundles):
        pass

    def apply_filters(self, object_list, filters=None):
        if filters:
            for k, v in filters.iteritems():
                if hasattr(self.Meta.object_class, k) and k in self.Meta.filtering:
                    prop = getattr(self.Meta.object_class, k)
                    if isinstance(prop, BooleanProperty):
                        v = v.lower().strip() == 'true'
                    object_list = object_list.filter(prop == v)
        return object_list


