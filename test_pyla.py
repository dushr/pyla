import unittest

import nose
import random
import string
import redis

from pyla import enteries
from pyla import fields


class QueueEntry(enteries.Entry):

    db = redis.Redis(db=3)

    country = fields.BaseField(index=True)
    category = fields.BaseField(index=True)
    language = fields.BaseField(index=True)
    id = fields.BaseField(primary=True)

def random_key(size):
    """ Generates a random key
    """
    return ''.join(random.choice(string.letters) for _ in range(size))

class TestPyla(unittest.TestCase):

    def setUp(self):
        r = redis.Redis(db=3)
        r.flushdb()


    def teardown(self):
        r = redis.Redis(db=3)
        r.flushdb()

    def test_serialize(self):

        entry_data = {
            'country': 10,
            'category': 'cars',
            'language': 'en',
            'id': random_key(32),
        }

        queue_entry = QueueEntry(**entry_data)

        self.assertDictEqual(queue_entry.serialize(), entry_data)

    def test_get_with_pk(self):

        pk = random_key(32)
        entry_data = {
            'country': 10,
            'category': 'cars',
            'language': 'en',
            'id': pk,
        }
        queue_entry = QueueEntry(**entry_data)
        queue_entry.save()

        saved_entry = QueueEntry.objects.get(pk)
        self.assertEqual(saved_entry.id, queue_entry.id)

    def test_orfilter(self):

        COUNT = 1000

        countries = [1,2,3,4,5,6,7,8,9,10,11,12,13,14]
        languages = ['en', 'ar', 'fr']
        categories = ['cars', 'items', 'property', 'jobs']
        for _ in range(COUNT):
            e = QueueEntry()
            e.country = random.choice(countries)
            e.category = random.choice(categories)
            e.language = random.choice(languages)
            e.id = random_key(32)
            e.save()

        for name, params in (('country', countries), ('language', languages), ('category', categories)):

            nose.tools.assert_equals(
                len(QueueEntry.objects.filter(**dict(((name, params),)))),
                1000
            )
