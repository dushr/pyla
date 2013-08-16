import copy
import time
import redis

import random
import string

import fields

db = redis.Redis()


class EntryMeta(type):
    """ The meta class for an Entry. 
    Copy overs the class level fields into an available dictionary and makes
    sure that there atleast one and only one primary key field assigned to the
    entry
    """

    def __new__(cls, name, bases, attr_dict):
        super_new = super(EntryMeta, cls).__new__
        # Get the parents of the class. This is going to be used to help
        # get the right inheritence.
        parents = [b for b in bases if isinstance(b, EntryMeta)]
        if not parents:
            return super_new(cls, name, bases, attr_dict)

        # We remove the fields from being available
        field_dict = dict([(k, attr_dict.pop(k)) for k, v in attr_dict.items() if isinstance(v, fields.BaseField)])
        # At this point we have inheritance! Basically without attr_dict, we
        # have the parent class of what we are about to create
        entry_class = super(EntryMeta, cls).__new__(cls, name, bases, attr_dict)


        # Get all the parent fields we have to do a copy over here, so that
        # every model points to a different dictionary of fields remeber that
        # class variables are shared even across sibling inherited classes
        base_fields = copy.deepcopy(getattr(entry_class, 'base_fields', {}))
        # This will still be accessible at a class level
        base_fields.update(field_dict)

        # Now check if there is one and only one primary field for the entry
        primary_fields = [(n,f) for n,f in base_fields.iteritems() if f.primary]
        if not(len(primary_fields)) == 1:
            # TODO: Raise a better error
            raise AttributeError('You can have one and only one primary field per entry')
        # Push this back into the class with updated content
        setattr(entry_class, 'base_fields', base_fields)

        return entry_class


class BaseEntry(object):
    """ The BaseEntry, overrides loads of magic method to make the interface
    awesome and allows us to save enteries with all the de-normalization magic.
    """

    __metaclass__ = EntryMeta

    def __init__(self, *args, **kwargs):
        """ Copies over the class level base_fields into instance variable
        called fields.
        """

        self.fields = copy.deepcopy(self.base_fields)         
        self._pk_field = None

    def __getattr__(self, name):
        """ First check, if the attribute that we are trying to get is a field
        otherwise fallback to normal getattribute.
        """
        try:
            field = self.__getattribute__('fields')[name] 
        except KeyError:
            return super(BaseEntry, self).__getattribute__(name)
        else:
            return field.value

    def __setattr__(self, name, value):
        """ First check, if we are trying to set the attribute to a field
        otherwise fallback to the normal setattr.
        """
        try:
            field = self.__dict__['fields'][name]
        except KeyError:
            super(BaseEntry, self).__setattr__(name, value)
        else:
            field.value = value

    @property
    def pk(self):
        """ Get the primary key value.
        """
        if not self._pk_field:
            self._pk_field = [f for f in self.fields.values() if f.primary][0]

        return self._pk_field.value

    @pk.setter
    def pk(self, value):
        """ Setter for primary key value.
        """
        if not self._pk_field:
            self._pk_field = [f for f in self.fields.values() if f.primary][0]

        self._pk_field.value = value

    def _generate_key(self):
        """ generates the key that we will be storing the item with.

        attr_name>value:attr_name>value:primary_key
        """
        fields = sorted([(n, f) for (n, f) in self.fields.iteritems() if not f.primary])
        field_entries = ['>'.join((n, str(f.value))) for n,f in fields] + [str(self.pk)]
        return ':'.join(field_entries)
        

    def save(self):
        """ Saves the entry in a sorted set and also creates indexes for fields
        that have to indexed.
        """
        save_time = time.time()
        queue_name = self.__class__.__name__
        key = self._generate_key()
        db.zadd(queue_name, key, save_time)    

        index_fields = [(n, f) for (n, f) in self.fields.iteritems() if f.index]

        for name, field in index_fields:
            index_queue_name = ':'.join([queue_name, name, str(field.value)])
            db.zadd(index_queue_name, key, save_time)

    @classmethod
    def filter(cls, **kwargs):

        if not set(cls.base_fields.keys()).issuperset(kwargs.keys()):
            raise ValueError

        queue_name = cls.__name__

        # Make the OR filters.
        # if the value is a list, we assume it is an OR filter
        or_filters = [(k, v) for k,v in kwargs.iteritems() if isinstance(v, list)]
        union_sets = []
        
        for name, values in or_filters:
            kwargs.pop(name)
            or_sets = [':'.join([queue_name, name, str(v)]) for v in values]
            or_set = '|'.join(or_sets)
            union_sets.append(or_set)
            if len(or_sets) > 1:
                db.zunionstore(or_set, or_sets)

        sets = [':'.join([queue_name, name, str(value)]) for name, value in kwargs.iteritems()]
        sets = sets + union_sets

        if len(sets) == 1: 
            return db.zrange(sets[0], 0, -1)
        
        filter_set = '&'.join(sets)
        db.zinterstore(filter_set, sets)

        return db.zrange(filter_set, 0, -1)

class Entry(BaseEntry):

    country = fields.BaseField(index=True)
    
    category = fields.BaseField(index=True)

    language = fields.BaseField(index=True)

    id = fields.BaseField(primary=True)

def random_key(size):
    """ Generates a random key
    """
    return ''.join(random.choice(string.letters) for _ in range(size))

if __name__ == '__main__':

    countries = [1,2,3,4]
    languages = ['en', 'ar', 'fr']
    categories = ['cars', 'items', 'property', 'jobs']
    for _ in range(1000):
        e = Entry()
        e.country = random.choice(countries)
        e.category = random.choice(categories)
        e.language = random.choice(languages)
        e.id = random_key(8)
        e.save()

    print e.filter(country=4, language='en')
