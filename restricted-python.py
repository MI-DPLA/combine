from RestrictedPython import compile_restricted#as compile
from RestrictedPython import safe_globals
import sys

source_code = """
def do_something():
    return "HI!"
"""

byte_code = compile_restricted(
        source_code,
        filename='<inline code>',
        mode='exec'
)
local = {}
exec(byte_code, safe_globals, local)
print(local['do_something']())

safe_list = ['lxml', 'lxml.etree']

def secureModule(mname, globals, locals, fromlist):
    if mname in safe_list:
        __import__(mname, globals, locals, fromlist)
    module = sys.modules[mname]
    return module

def load_module(module, mname, mnameparts, globals, locals, fromlist):
    while mnameparts:
        nextname = mnameparts.pop(0)
        if mname is None:
            mname = nextname
        else:
            mname = '%s.%s' % (mname, nextname)
        # import (if not already imported) and  check for MSI
        nextmodule = secureModule(mname, globals, locals, fromlist)
        if nextmodule is None:  # not allowed
            return
        module = nextmodule
    return module

def guarded_import(mname, globals=None, locals=None, fromlist=None, level=None):
    if fromlist is None:
        fromlist = ()
    if globals is None:
        globals = {}
    if locals is None:
        locals = {}

    mnameparts = mname.split('.')
    module = load_module(None, None, mnameparts, globals, locals, fromlist)
    if module is None:
        raise Exception("import of '%s' is unauthorized" % mname)
    for name in fromlist:
        v = getattr(module, name, None)
        if v is None:
            v = load_module(module, mname, [name], globals, locals, fromlist)
    else:
        return __import__(mname, globals, locals, fromlist)


safe_globals['__builtins__']['__import__'] = guarded_import

source_code = """
from lxml import etree
from lxml import nonsense
"""

byte_code = compile_restricted(source_code, filename='<inline code>', mode='exec')
exec(byte_code, safe_globals, local)
