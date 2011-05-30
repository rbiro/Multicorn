# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under 3-clause BSD

"""
Aliases test.

Test the aliases backend.

"""

# Nose redefines assert_raises
# pylint: disable=E0611
from nose.tools import eq_, assert_raises, raises
# pylint: enable=E0611

from multicorn.item import Item
from multicorn.property import Property
from multicorn.request import Condition, And, Or, Not
from multicorn.access_point.memory import Memory
from multicorn.access_point.aliases import AliasedItem, Aliases
from ..common import make_site, run_common



class DummyAP(object):
    """Dummy access point."""


def make_ap():
    """Create a simple access point."""
    underlying_access_point = Memory(
        {"id": Property(int), "nom": Property(unicode)}, ("id",))
    return Aliases(underlying_access_point, {"name": "nom"})

@run_common
def test_alias():
    """Launch common tests for aliases."""
    return make_ap()

def memory_make_ap():
    """Create a simple access point."""
    return Memory({"id": Property(int), "name": Property(unicode)}, ("id",))

def test_aliased_item():
    """Test aliases for various items."""
    dummy_underlying_access_point = DummyAP()
    dummy_access_point = DummyAP()

    # Defining various AP attributes
    # pylint: disable=W0201
    dummy_underlying_access_point.properties = {
        "FOO": Property(int), "other": Property(int)}
    dummy_underlying_access_point.identity_properties = ()
    dummy_access_point.properties = {
        "foo": Property(int), "other": Property(int)}
    dummy_access_point.aliases = {"foo": "FOO"}
    dummy_access_point.reversed_aliases = {"FOO": "foo"}
    # pylint: enable=W0201

    wrapped_item = Item(dummy_underlying_access_point, {"FOO": 9, "other": 0})
    item = AliasedItem(dummy_access_point, wrapped_item)
    # Aliased property
    eq_(wrapped_item["FOO"], 9)
    item["foo"] = 1
    eq_(wrapped_item["FOO"], 1)
    # Old names are masked
    assert "FOO" not in item
    # Non aliased property
    eq_(wrapped_item["other"], 0)
    item["other"] = 2
    eq_(wrapped_item["other"], 2)
    # Other attr
    assert item.access_point is dummy_access_point
    # MultiDict-like method, aliased property
    item.setlist("foo", (2, 3))
    eq_(item["foo"], 2)
    eq_(item.getlist("foo"), (2, 3))
    eq_(wrapped_item["FOO"], 2)
    eq_(wrapped_item.getlist("FOO"), (2, 3))

def test_alias_request():
    """Test request translations."""
    access_point = Aliases(
        Memory({"foo": Property(str), "bar": Property(str)}, ("foo",)),
        {"foo": "FOO", "bar": "BAR"})
    eq_(access_point._alias_request(Condition("foo", "=", 4)),
        Condition("FOO", "=", 4))
    eq_(access_point._alias_request(Condition("other", "=", 7)),
        Condition("other", "=", 7))
    eq_(access_point._alias_request(
            Not(Or(Condition("other", "=", 7),
                   Condition("bar", "!=", 1)))),
        Not(Or(Condition("other", "=", 7),
               Condition("BAR", "!=", 1))))
    eq_(access_point._alias_request(
            And(Condition("foo", "=", 4),
                Not(Or(Condition("other", "=", 7),
                       Condition("bar", "!=", 1))))),
        And(Condition("FOO", "=", 4),
            Not(Or(Condition("other", "=", 7),
                   Condition("BAR", "!=", 1)))))

def test_aliased_memory():
    """Test aliases on a memory access point."""
    site = make_site(memory_make_ap(), fill=True)
    underlying_access_point = site.access_points["things"]
    access_point = Aliases(underlying_access_point, {"nom": "name"})
    site.register("aliased", access_point)
    
    results = site.search("aliased", {"nom": "bar"})
    eq_(set(item["id"] for item in results), set([2, 3]))

    # Old names are masked
    assert_raises(KeyError, site.search, "aliased", {"name": "bar"})

@raises(KeyError)
def test_bad_key():
    """Wrapping an inexistant key raises an error when property is asked."""
    site = make_site(memory_make_ap(), fill=True)
    underlying_access_point = site.access_points["things"]
    access_point = Aliases(underlying_access_point, {"test": "unavailable"})
    site.register("aliased", access_point)
    item = site.create("aliased", {"id": 1, "name": "spam"})

    # This access should raise a KeyError
    # pylint: disable=W0104
    item["test"]
    # pylint: enable=W0104

def test_properties():
    """List the properties of an aliased item."""
    site = make_site(memory_make_ap(), fill=True)
    underlying_access_point = site.access_points["things"]
    access_point = Aliases(underlying_access_point, {"nom": "name"})
    site.register("aliased", access_point)

    item = site.open("aliased", {"id": 1})
    item_properties = [prop for prop in item]
    eq_(set(item_properties), set(("nom", "id")))