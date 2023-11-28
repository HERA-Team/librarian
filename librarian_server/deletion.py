
class DeletionPolicy (object):
    """A simple enumeration of symbolic constants for the "deletion_policy"
    column in the FileInstance table.

    """
    DISALLOWED = 0
    ALLOWED = 1

    def __init__(self): assert False, 'instantiation of enum not allowed'

    @classmethod
    def parse_safe(cls, text):
        if text == 'disallowed':
            return cls.DISALLOWED
        if text == 'allowed':
            return cls.ALLOWED

        logger.warn('unrecognized deletion policy %r; using DISALLOWED', text)
        return cls.DISALLOWED

    @classmethod
    def textualize(cls, value):
        if value == cls.DISALLOWED:
            return 'disallowed'
        if value == cls.ALLOWED:
            return 'allowed'
        return '???(%r)' % (value, )
