from distutils.core import setup

setup(
        name = 'aCT-client',
        version = '1.0',
        url = 'https://www.act.com',
        maintainer = 'Jakob Merljak',
        maintainer_email = 'jakob.merljak@ijs.si',
        package_dir = {'': 'src'},
        py_modules = ['config'],
        scripts = [
            'src/actproxy.py',
            'src/actlistproxies.py',
            'src/actdeleteproxy.py',
            'src/actstat.py',
            'src/actclean.py',
            'src/actfetch.py',
            'src/actkill.py',
            'src/actresub.py',
            'src/actsub.py',
            'src/actget.py'
        ]
)
