# webstorageS3
Simple Framework to archive binary blobs, files and archives on S3

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
