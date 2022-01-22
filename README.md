# webstorageS3
Simple Framework to archive binary blobs, files and archives on any S3 server.

Data will be deduplicated on block (1 megabyte) and file level.

There is actually no way to delete something.

## blockstorage bucket

This bucket is used to store chunks of max 1024 * 1024 bytes.
The original data will be split in this chunks and stored under their SHA1 cheksum

## filestorage bucket

Every file will be split into chunks of max 1024 * 1024 bytes, these blobs are stored in blockstorage bucket.
The list of checksums of each individual block will be stored in filestorage bucket named by the SHA1 checksum of the whole file.

## webstorage bucket

In this bucket there will be stored json structures with file and directory informations.

Given an S3 Backend Storage like amazon S3 or azure or Scality S3 Server
you can store arbitrary data with this framework on them adding
- block level deduplication
- file level deduplication

any Data will be split in chunks of max 1024 * 1024 bytes,
these chunks will be stored in the BlockStorage Bucket
- every Block will be named by its checksum

any File uploaded will be split into blocks of 1024 * 1024,
these blocks are store in Blockstorage, and the recipe to rebuild this
file will be stored named by file checksum in FileStorage Bucket
- every Filesystem Metadata will be named by file checksum

Archived made by wstar will store the data in FileStorage and Blockstorage
and keep metadata of archived files like a tar archive.
this metadata will be stored in WebArchive Bucket.
- every WebstorageArchive will be named by checksum of metadata
