import copy
import time
import redis

import random
import string

db = redis.Redis()


class BaseField(object):

    def __init__(self, default=None, index=False, primary=False):

        self.index = index
        self.primary = primary
        self._value = default  

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value):
        self._value = value
    

class EntryBuilder(type):

    def __new__(cls, name, bases, attr_dict):
        super_new = super(EntryBuilder, cls).__new__
        # Get the parents of the class. This is going to be used to help
        # get the right inheritence.
        parents = [b for b in bases if isinstance(b, EntryBuilder)]
        if not parents:
            return super_new(cls, name, bases, attr_dict)

        # We remove the fields from being available
        field_dict = dict([(k, attr_dict.pop(k)) for k, v in attr_dict.items() if isinstance(v, BaseField)])
        # At this point we have inheritance! Basically without attr_dict, we
        # have the parent class of what we are about to create
        model_class = super(EntryBuilder, cls).__new__(cls, name, bases, attr_dict)


        # Get all the parent fields we have to do a copy over here, so that
        # every model points to a different dictionary of fields remeber that
        # class variables are shared even across sibling inherited classes
        base_fields = copy.deepcopy(getattr(model_class, 'base_fields', {}))
        # This will still be accessible at a class level
        base_fields.update(field_dict)

        # Now check if there is one and only one primary field for the entry
        primary_fields = [(n,f) for n,f in base_fields.iteritems() if f.primary]
        if not(len(primary_fields)) == 1:
            # ToDo: Raise a better error
            raise AttributeError('You can have one and only one primary field per entry')
        # Push this back into the class with updated content
        setattr(model_class, 'base_fields', base_fields)

        return model_class


class BaseEntry(object):

    __metaclass__ = EntryBuilder

    def __init__(self, *args, **kwargs):
        self.fields = copy.deepcopy(self.base_fields)         
        self._pk_field = None

    def __getattr__(self, name):
    
        try:
            field = self.__getattribute__('fields')[name] 
        except KeyError:
            return super(BaseEntry, self).__getattribute__(name)
        else:
            return field.value

    def __setattr__(self, name, value):

        try:
            field = self.__dict__['fields'][name]
        except KeyError:
            super(BaseEntry, self).__setattr__(name, value)
        else:
            field.value = value

    @property
    def pk(self):
        if not self._pk_field:
            self._pk_field = [f for f in self.fields.values() if f.primary][0]

        return self._pk_field.value


    def _generate_entry(self):
                
        fields = sorted([(n, f) for (n, f) in self.fields.iteritems() if not f.primary])
        field_entries = ['>'.join((n, str(f.value))) for n,f in fields] + [str(self.pk)]
        return ':'.join(field_entries)
        

    def save(self):
        save_time = time.time()
        queue_name = self.__class__.__name__
        entry = self._generate_entry()
        db.zadd(queue_name, entry, save_time)    

        index_fields = [(n, f) for (n, f) in self.fields.iteritems() if f.index]

        for name, field in index_fields:
            index_queue_name = ':'.join([queue_name, name, str(field.value)])
            db.zadd(index_queue_name, entry, save_time)

    @classmethod
    def filter(cls, **kwargs):

        if not set(cls.base_fields.keys()).issuperset(kwargs.keys()):
            raise ValueError

        queue_name = cls.__name__

        # Make the OR filters.
        # if the value is a list, we assume it is an OR filter
        or_filters = [(k, v) for k,v in kwargs.iteritems() if isinstance(v, list)]

        print or_filters
        union_sets = []
        for name, values in or_filters:
            kwargs.pop(name)
            or_sets = [':'.join([queue_name, name, str(v)]) for v in values]
            or_set = '|'.join(or_sets)
            union_sets.append(or_set)
            if len(or_sets) > 1:
                db.zunionstore(or_set, or_sets)

        print union_sets


        sets = [':'.join([queue_name, name, str(value)]) for name, value in kwargs.iteritems()]

        sets = sets + union_sets

        print sets

        if len(sets) == 1: 
            return db.zrange(sets[0], 0, -1)
        
        filter_set = '&'.join(sets)

        db.zinterstore(filter_set, sets)

        return db.zrange(filter_set, 0, -1)

class Entry(BaseEntry):

    country = BaseField(index=True)
    
    category = BaseField(index=True)

    language = BaseField(index=True)

    id = BaseField(primary=True)

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
        print e._generate_entry()
        e.save()

    print e.filter(country=4, language='en')
