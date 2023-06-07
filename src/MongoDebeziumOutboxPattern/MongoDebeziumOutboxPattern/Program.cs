// See https://aka.ms/new-console-template for more information

using MongoDB.Driver;
using MongoDebeziumOutboxPattern;

var client = new MongoClient("mongodb://localhost:27017");
var userService = new UserService(client);
await userService.CreateUserAsync("Adam");

