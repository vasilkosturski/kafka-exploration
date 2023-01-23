using Confluent.Kafka;
using RocksDbSharp;
using Streamiz.Kafka.Net.SerDes;

namespace OrdersValidationService;

public class RockDBReader
{
    public static void Read()
    {
        ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions();
        //this.writeOptions = new WriteOptions();
        BlockBasedTableOptions table_options = new BlockBasedTableOptions();
        //RocksDbOptions rocksDbOptions = new RocksDbOptions(dbOptions, columnFamilyOptions);
        
        DbOptions dbOptions = new DbOptions();

        using (var db = RocksDb.Open(dbOptions, @"./test-app/1-0/rocksdb/orders-products-quantities", new ColumnFamilies(columnFamilyOptions)))
        {
            // Using strings below, but can also use byte arrays for both keys and values
            //db.Put("key", "value");
            //string value = db.MultiGet(1)

            var keyToInsert = 100;
            var valueToInsert = 200;
            
            var keyAsBytes = ToBytes(0);
            var valueAsBytes = ToBytes(valueToInsert);

            //db.Put(keyAsBytes, valueAsBytes);

            var hasKey = db.HasKey(keyAsBytes);
            
            var dbResponse = db.Get(keyAsBytes, db.GetDefaultColumnFamily());
            
            var serdes = new ValueAndTimestampSerDes<int>(new Int32SerDes());
            var res = serdes.Deserialize(dbResponse, new SerializationContext());

            var a = 10;

            //var res = FromBytes(dbResponse);
            //var res = System.Text.Encoding.UTF8.GetString(dbResponse);;
        }
    }

    private static byte[] ToBytes(int from)
    {
        var intBytes = BitConverter.GetBytes(from);
        if (BitConverter.IsLittleEndian)
            Array.Reverse(intBytes);
        return intBytes;
    }
    
    private static int FromBytes(byte[] from)
    {
        if (BitConverter.IsLittleEndian)
            Array.Reverse(from);

        return BitConverter.ToInt32(from, 0);
    }
}