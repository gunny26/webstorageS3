from distutils.core import setup, Extension
#from Cython.Build import cythonize
import sys, string, os
import shutil

args = {"name": "webstorageS3",
        "author": "Arthur Messner",
        "author_email": "arthur.messner@gmail.com",
        "description": "WebStorage Archiving System on S3 Backend",
        "url" : "https://github.com/gunny26/webstorageS3",
        "long_description": __doc__,
        "platforms": ["any", ],
        "license": "LGPLv2",
        "packages": ["webstorageS3"],
        "scripts": ["bin/wstar.py", "bin/fget.py", "bin/fput.py"],
        "package_dir": {
            "webstorageS3": "webstorageS3",
            },
        "requires" : ["requests", "boto3", "yaml"],
        "version" : "1.0.0",
        }
setup(**args)
