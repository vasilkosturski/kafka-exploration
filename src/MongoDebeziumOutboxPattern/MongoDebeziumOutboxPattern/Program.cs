using MongoDB.Bson.Serialization.Conventions;
using MongoDebeziumOutboxPattern;

MongoSetup();

await new UserService().CreateUserAsync();

void MongoSetup()
{
    var conventionPack = new ConventionPack { new CamelCaseElementNameConvention() };
    ConventionRegistry.Register("camelCase", conventionPack, _ => true);
}