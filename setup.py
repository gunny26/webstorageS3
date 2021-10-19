import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()


setuptools.setup(
     name='webstorageS3',
     version='1.2.1',
     scripts=["bin/wstar.py", "bin/fget.py", "bin/fput.py", "bin/bstool.py", "bin/fstool.py"] ,
     author="Arthur Messner",
     author_email="arthur.messner@gmail.com",
     description="WebStorage Archiving System on S3 Backend",
     long_description=long_description,
     long_description_content_type="text/markdown",
     url="https://github.com/gunny26/webstorageS3",
     packages=setuptools.find_packages(),
     classifiers=[
         "Programming Language :: Python :: 3",
         "License :: OSI Approved :: MIT License",
         "Operating System :: OS Independent",
     ],
    requires=["requests", "boto3", "yaml"],
 )
