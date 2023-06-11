using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Conventions;
using MongoDB.Bson.Serialization.Serializers;
using MongoDebeziumOutboxPattern;

MongoSetup();

await new UserService().CreateUserAsync();

void MongoSetup()
{
    var conventionPack = new ConventionPack { new CamelCaseElementNameConvention() };
    ConventionRegistry.Register("camelCase", conventionPack, t => true);

    var supportedTypes = new[] { typeof(User) };
    var objectSerializer = new ObjectSerializer(type => supportedTypes.Contains(type));
    BsonSerializer.RegisterSerializer(objectSerializer);
}