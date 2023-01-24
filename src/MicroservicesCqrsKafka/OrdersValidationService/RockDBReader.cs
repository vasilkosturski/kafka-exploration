using Confluent.Kafka;
using RocksDbSharp;
using Streamiz.Kafka.Net.SerDes;

namespace OrdersValidationService;

public class RockDBReader
{
    public static void Read()
    {
        var dbOptions = new DbOptions();
        using (var db = RocksDb.Open(dbOptions, @"./test-app/1-0/rocksdb/orders-products-quantities", 
                   new ColumnFamilies(new ColumnFamilyOptions())))
        {
            // Using strings below, but can also use byte arrays for both keys and values
            //db.Put("key", "value");
            //string value = db.MultiGet(1)
            
            //db.NewIterator()

            var keyToInsert = 100;
            var valueToInsert = 200;

            var intSerDes = new Int32SerDes();

            // var keyAsBytes = ToBytes(1);

            var key = 1;
            
            var keyAsBytes = intSerDes.Serialize(key, new SerializationContext());

            var hasKey = db.HasKey(keyAsBytes);

            var dbResponse = db.Get(keyAsBytes, db.GetDefaultColumnFamily());
            
            var serdes = new ValueAndTimestampSerDes<int>(new Int32SerDes());
            var res = serdes.Deserialize(dbResponse, new SerializationContext());

            var a = 10;

            //var res = FromBytes(dbResponse);
            //var res = System.Text.Encoding.UTF8.GetString(dbResponse);;
        }
    }
}