using Confluent.Kafka;
using RocksDbSharp;
using Streamiz.Kafka.Net.SerDes;
using static Common.Utils;
using static Common.Constants;

namespace RocksDbReader;

public static class Program
{
    public static void Main(string[] args)
    {
        PrintValueForKey("1-0", 0);
        PrintValueForKey("1-1", 1);
    }
    
    private static void PrintValueForKey(string taskName, int key)
    {
        var rootStateDirectory = GetStateDirectory();

        using var db = RocksDb.Open(new DbOptions(), 
            $@"{rootStateDirectory}\{ApplicationName}\{taskName}\rocksdb\{StateStoreName}", 
            new ColumnFamilies(new ColumnFamilyOptions()));
        
        var keySerialized = new Int32SerDes().Serialize(key, new SerializationContext());
            
        var dbByteResponse = db.Get(keySerialized, db.GetDefaultColumnFamily());

        var dbResponse = new ValueAndTimestampSerDes<int>(new Int32SerDes())
            .Deserialize(dbByteResponse, new SerializationContext());
            
        Console.WriteLine($"RockDB Data. Task: {taskName}. Key: {key}. Value: {dbResponse.Value}");
    }
}