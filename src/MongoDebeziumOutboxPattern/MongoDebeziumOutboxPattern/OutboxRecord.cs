using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace MongoDebeziumOutboxPattern;

public class OutboxRecord
{
    [BsonId]
    [BsonRepresentation(BsonType.ObjectId)]
    public string Id { get; set; }
    
    [BsonElement("aggregatetype")]
    public string AggregateType { get; set; }
    
    [BsonElement("aggregateid")]
    public string AggregateId { get; set; }
    
    public string Type { get; set; }
 
    public string Payload { get; set; }
}