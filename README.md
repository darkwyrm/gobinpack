# GoBinPack

Go implementation of the JBinPack data serialization format.

## Usage

TODO

## About JBitPack

JBitPack is yet another format for data serialization inspired by the [Netstring](https://en.wikipedia.org/wiki/Netstring) format. It is a lightweight self-documenting binary format meant to be used in messaging APIs -- nearly as flexible as JSON, handling binary data much more efficiently, and yet not as complex as many other binary formats currently available. It centers around segments of data which start with a 1-byte data type code. For fixed-length data types, such as 16-bit signed integers, the data follows immediately afterward. For example, the string of bytes `05 00 00 FF FF` is a segment containing a 32-bit signed integer -- type code 5 -- followed by the 32-bit value 65535.

Variable-length data types use a type code, a size code, and then the data. The regular String and Binary types support individual segments of up to 64KiB. For greater capacity needs there are the HugeString and HugeBinary types, which supports up to 16EiB. Strings are required to be UTF-8 compliant are are not null-terminated. The string "ABC123" would be encoded as thus: `0e 00 06 41 42 43 31 32 33`.

JBitPack also supports container types in a basic way. Lists are stored using a list type indicator followed by a series of regular data segments. The list indicator segment itself is stored much like a uint16 field: a list type code followed by an unsigned 16-bit integer which contains the number of items in the list. A list can contain any type of data segments and the segments are not required to all be the same type. Hash maps, or dictionaries, are handled as a special type of list. The map indicator segment contains the number of key-value pairs in the container, not the number of segments. Similar to JSON, maps are required to use String fields for keys. Values paired with those keys may be of any type except List and Map. Container types may not be nested for complexity reasons. Both lists and maps come in two types: regular and large. Regular containers have a 16-bit count indicator and, thus, have a maximum limit of 65535 items. Large ones use a 64-bit count indicator and are, for practical purposes, not limited in size.

