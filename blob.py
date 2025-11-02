#!/usr/bin/env python3

from collections import namedtuple, UserList
from pathlib import Path

class Blob:
    """A blob of binary data or information

    Fields:

    sig: The four-character ASCII type identifier

    content: The payload. May or may not be converted to a native Python type,
    depending on if `dtype` is specified, or `convert` has been called. If not,
    is a `bytes` objext.

    contentRaw: The `bytes` object representing the data as written on disk.

    size: The size of the data, as wrtten to disk, in bytes
    """


    def __init__( self, sig, content=None, size=None, dtype=None ):
        """Create a new Blob.

        sig: The 4-character type identifier

        content: The payload. If not specified, the blob is a null-length
        "marker" blob. This may be an `int`, `str`, or `bool`, in which it will
        be converted as appropriate when written to disk, or `bytes` for binary
        data, or raw data read from a file. In the latter case, specifying a
        `dtype` will convert the read data to the given type.

        size: The size, in bytes, as written to disk. Generally not needed, as
        it can be autodetected from the content, though this can be specified
        to manually set the length of an intger

        dtype: The type (as a Python type, like `int` or `str`) that the
        content should be converted to.
        """

        self.sig = sig
        self.content = content

        if isinstance( content, bytes ):
            self.contentRaw = content
        elif isinstance( content, int ):
            if size == None: size = 4
            self.contentRaw = content.to_bytes(
                    size, byteorder='big', signed=True )
        elif isinstance( content, bool ):
            if size == None: size = 1
            self.contentRaw = int( content ).to_bytes(
                    size, byteorder='big', signed=True )
        elif isinstance( content, str ):
            self.contentRaw = content.encode()
        elif content is None:
            size = 0
            self.contentRaw = None

        if size == None: size = len( self.contentRaw )

        self.size = size
        if dtype:
            self.convert( dtype )

    def getAs( self, dtype ):
        """Return the payload as the given type"""

        if dtype is int:
            return int.from_bytes( self.contentRaw, byteorder='big' )
        if dtype is str:
            try:
                return self.contentRaw.decode()
            except:
                return str( self.contentRaw )
        if dtype is bytes:
            return self.contentRaw
        if dtype is bool:
            return bool( self.getAs( int ))
        if dtype is None:
            return None
        return self.contentRaw

    def convert( self, dtype ):
        """Convert our data field to the given type"""
        self.content = self.getAs( dtype )
        return self.content

    def __str__( self ):
        """Return the content as a string.

        If the content is binary data, simply returns a placeholder. Otherwise,
        returns the content converted to a `str`
        """

        if isinstance( self.content, bytes ):
            return "<DATA>"
        return str( self.content )

    def __repr__( self ):
        """Return the content in Python representation"""

        if isinstance( self.content, bytes ):
            return "bytes(...)"
        return repr( self.content )

    def __int__( self ):
        """Return the content converted to an int"""

        if isinstance( self.content, int ):
            return self.content
        return self.getAs( int )

    def __bool__( self ):
        """Return the content converted to a bool"""
        return bool( int( self ))

    def __bytes__( self ):
        """Return the ray binary content"""
        return self.contentRaw

    def encode( self ):
        """Encode blob into format suitable for writing.

        This will create a `bytearray` with the 4 byte ASCII identifier, a 4
        byte size, and the data itself."""
        output = bytearray()
        output.extend( self.sig.encode( 'ASCII' ))
        output.extend( self.size.to_bytes( 4, byteorder='big' ))
        if self.size:
            output.extend( self.contentRaw )
        return output

class BlobGroup( Blob, UserList ):
    """A group of Blobs.

    This is a 'hybrid' of a `list` and a `dict`. It subclasses UserList, so
    accessing it as a list works as expeted: It contains all the child blobs.
    It also has several `dict`-like accessors: It can be subscripted with a
    `str`, which returns the first child of the given signature. Subscripting
    with an `int` returns the child with that index, same as a list. keys() and
    items() work as expected for a dict; values() reuturns the children as a
    list.

    Note that the first time a `dict`-style accessor is called, the group's 'keys'
    are stored in a cache, and not updated again. Consequently, `dict`-style
    accessors should not be used until the group is fully populated, or,
    alternately, clearDict() should be called after any changed.

    Fields:

    sig: The identifier of the opening Blob

    data: A `list` of the children

    dict: A `dict` mapping the first child with the given signature to the
    child itself.

    content: The data in the header blob. Normally this is the size of the
    group, if written in standard format, but could contain other information
    for non-standard group formats. This is only used when reading a
    non-standard group; when written, it is encoded in standard format.

    terminator: If a non-standard group is read, this may contain the ending
    marker blob
    """
    def __init__( self, sig, children=None, content=None ):
        super().__init__( sig, content )

        self.data = []
        if children:
            self.data = children
        self.dict = {}
        self.terminator = None
        
    def clearDict( self ):
        """Clear the dictionary lookup table."""

        self.dict = {}

    def genDict( self ):
        """Generate the dictionary lookup table.

        This is called the first time one of the `dict`-style access methods is
        used."""

        if not self.dict:
            for blob in self:
                self.dict.setdefault( blob.sig, blob )

    def __contains__( self, key ):
        self.genDict()
        return key in self.dict

    def __getitem__( self, i ):
        if isinstance( i, str ):
            self.genDict()
            return self.dict[i]
        else:
            return super().__getitem__( i )

    def __str__( self ):
        return '[' + ', '.join(
                [f"{b.sig}: {repr( b )}" for b in self] ) + ']'
    
    def keys( self ):
        self.genDict()
        return self.dict.keys()
    
    def items( self ):
        self.genDict()
        return self.dict.items()
    
    def values( self ):
        return self.data

    def encode( self ):
        """Encode the group and its children appropriately for writing"""

        output = bytearray()
        output.extend( Blob( self.sig, len( self.data )).encode() )
        for b in self.data:
            output.extend( b.encode() )
        return output

class BlobFile:
    """A file of Blob objects.

    A BlobFile can be created and passed an open file descriptor, or can be
    opened with the open() class method.

    A BlobFile can be read using the read() method, which returns the next Blob
    or BlobGroup. It can also be written with the write() method to write a
    Blob or BlobGroup.

    This implements the context manager protocol, and can be `with`ed to
    automatically close the file when it goes out of scope.

    It also implements the iterator protocol, so it can be `for`ed to traverse
    a file's contents.

    A BlobFile can be created with a `spec`, which defines the specification of
    each type of blob in the file, as a `dict`. For each blob signature type
    (the key), the type of data it contains (the value) is given as a Python
    type:

    None: Zero-byte "marker".
    int: Two's complement, big-endian integer.
    bool: Boolean.
    str: String.
    bytes: Raw binary data.
    list: A group, in "standard" format.

    While not exactly standard, two additional "types" can be given by the spec:

    An `int`: A group, of exactly the given number of elements.
    A `str`: A group, which continues until a blob of the given signature is
    encountered.

    If either of these formats are used, the BlobGroup containing such a group
    will have its `content` field set to the content of the opening blob. In a
    group where the end is demarcated by a given blob type, the end marker will
    be stored in the `terminator` field of the BlobGroup

    A spec is only used when reading a file. When writing, the appropriate type
    is determined by the type of data passed to the Blob constructors.

    A BlobFile may be created with a filetype; this is the 4-character ASCII
    identifier identifying the file type in a "standard" blob header. On a
    writable file, this will cause a standard header to be written; on a
    readable file, this checks for the proper header and raises an exception if
    it doesn't match.

    Additionally, a version for the file header and the blob header may also be
    specified. On a writable file, the header blobs will be given these
    versions. On a readable file, an exception is raised if these versions are
    less than what was read from the file (implying a newer and incompatible
    file format)

    If a filetype is specified, the BlobFile will contain the fields `header`
    and `blobheader`, which will contain the blobs making up the header; they
    will not be returned with a read().
    """
    def __init__( self, fd, spec=None,
                 filetype=None, version=None, blobver=None ):
        """Create a BlobFile reader or writer.

        fd: File descriptor of open file
        spec: The specification of the file (see class documentation)
        """

        self.fd = fd
        if spec:
            self.spec = spec
        else:
            self.spec = {}

        if fd.readable() and filetype:
            blobheader = self.read()
            if blobheader.sig != 'BLOB':
                raise FileTypeError( 'Not a blob file' )
            if (blobver is not None and blobheader.content
                and blobheader.getAs( int ) < blobver):
                raise VersionError( 'Old blob version' )
            header = self.read()
            if header.sig != filetype:
                raise FileTypeError( 'Wrong file type' )
            if (version is not None and header.content
                and header.getAs( int ) > version):
                raise VersionError( 'Old file version' )
        elif fd.writable() and filetype:
            blobheader = Blob( 'BLOB', blobver )
            self.write( blobheader )
            header = Blob( filetype, version )
            self.write( header )
        else:
            blobheader = None
            header = None

        if blobheader and blobheader.content: blobheader.convert( int )
        if header and header.content: header.convert( int )
        self.blobheader = blobheader
        self.header = header

    def __enter__( self ):
        return self

    def __exit__( self, type, value, traceback ):
        self.fd.__exit__( type, value, traceback )

    @classmethod
    def open( cls, path, mode='rb', spec=None,
             filetype=None, version=None, blobver=None ):
        """Open the given file and return a BlobFile object.

        path: Path to the file

        mode: The file mode. This is probably going to be 'rb' for reading, or
        'wb' for writing.

        spec: A dict giving the format of the file. Data will be converted to
        the appropriate Python types as read.
        """
        fd = open( path, mode )
        fd.__enter__()
        return cls( fd, spec, filetype, version, blobver )

    def close( self ):
        """Close the file"""
        self.fd.close()

    def write( self, blob ):
        """Write a Blob to the stream."""

        self.fd.write( blob.encode() )

    def next( self ):
        """(Depricated) Synonym for read()"""

        return self.read()

    def read( self ):
        """Read the next Blob or BlobGroup. Returns None at EOF.

        Note that checking for "truthiness" of the returned object is
        technically not adequate to detect EOF, as an empty BlobGroup is
        considered "false", as it's an empty list
        """
        fh = self.fd
        sigraw = fh.read( 4 )
        if not sigraw:
            return None # EOF
        sig = sigraw.decode()
        size = int.from_bytes( fh.read( 4 ), byteorder='big' ) 
        if size:
            data = fh.read( size )
        else:
            data = None

        blob = Blob( sig, data )
        btype = self.spec.get( sig )
        if btype is list:
            # Blob is a group, length specified by opening blob
            grp = BlobGroup( blob.sig )
            for i in range( int( blob )):
                grp.append( self.read() )
            return grp
        elif isinstance( btype, int ):
            # Blob is a group of a fixed length
            grp = BlobGroup( blob.sig, content=blob.content )
            for i in range( btype ):
                grp.append( self.read() )
            return grp
        elif isinstance( btype, str ):
            # Blob is a group with an end marker
            grp = BlobGroup( blob.sig, content=blob.content )
            blob = self.read()
            while blob.sig != btype:
                grp.append( blob )
                blob = self.read()
            grp.terminator = blob
            return grp
        else:
            # Blob is a single entity
            if size != 0 and btype != None:
                blob.convert( btype )
        return blob

    def __iter__( self ):
        return self

    def __next__( self ):
        next = self.read()
        if next == None:
            raise StopIteration
        return next

class VersionError( RuntimeError ):
    """Raised when a file is an unrecognized version"""

    def __init__( self, desc ):
        super().__init__( desc )

class FileTypeError( RuntimeError ):
    """Raised when a file is not the specified type"""

    def __init__( self, desc ):
        super().__init__( desc )



