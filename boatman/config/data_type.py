from enum import Enum, unique


@unique
class DataType(Enum):
    "Lists all Data Types"
    UInt8 = "uint8"
    UInt16 = "uint16"
    UInt32 = "uint32"
    UInt64 = "uint64"
    UInt256 = "uint256"
    Int8 = "int8"
    Int16 = "int16"
    Int32 = "int32"
    Int64 = "int64"
    Int128 = "int128"
    Int256 = "int256"
    Float32 = "float"
    Float64 = "double"
    BOOLEAN = "boolean"
    STRING = "string"
    UUID = "uuid"
    DATE = "date"
    DATETIME = "datetime"
    ARRAY = "array"
