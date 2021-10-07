using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Management;
using NUnit.Framework;

namespace Frends.AzureServiceBus.Tests
{
    public class EntitySpec
    {
        public QueueOrTopic EntityType { get; set; }
        public string Name { get; set; }
    }
    public class ServiceBusTests
    {
        private string _connectionString;
        private string _queueName;
        private string _subscriptionName;
        private string _queueName2;
        private string _queueName3;
        private ManagementClient _managementClient;
        private IList<EntitySpec> entitiesToDelete;

        [OneTimeSetUp]
        public void FixtureSetup()
        {
            _connectionString = ConfigurationManager.AppSettings["ServiceBusConnectionString"];
            if (string.IsNullOrEmpty(_connectionString))
            {
                throw new Exception("ServiceBusConnectionString app setting needs to be specified.");
            }
            _managementClient = new ManagementClient(_connectionString);
        }

        [SetUp]
        public void Setup()
        {
            _queueName = GetRandomEntityName();
            _subscriptionName = GetRandomEntityName();
            _queueName2 = "frends_processlog";
            _queueName3 = "frends_routinetriggers_default";
            entitiesToDelete = new List<EntitySpec>();
        }

        private static string GetRandomEntityName()
        {
            return Path.GetFileNameWithoutExtension(Path.GetTempFileName());
        }

        [TearDown]
        public async Task TearDown()
        {
            foreach (var entity in entitiesToDelete.GroupBy(e => new {e.Name, e.EntityType}).Select(e => e.First()))
            {
                switch (entity.EntityType)
                {
                    case QueueOrTopic.Queue:
                        if (await _managementClient.QueueExistsAsync(entity.Name))
                        {
                            await _managementClient.DeleteQueueAsync(entity.Name);
                        }
                        break;
                    case QueueOrTopic.Topic:
                        await _managementClient.DeleteTopicAsync(entity.Name);
                        break;
                    default:
                        break;
                }
            }
        }

        public async Task<SendResult> SendMessage(string entityPath, string data = "foobar", BodySerializationType serializationType = BodySerializationType.String, bool createEntity = true, QueueOrTopic queueOrTopic = QueueOrTopic.Queue, Encoding encoding = null, string contentType = null, Dictionary<string, string> properties = null )
        {
            encoding = encoding ?? Encoding.UTF8;
            entitiesToDelete.Add(new EntitySpec {EntityType = queueOrTopic, Name = entityPath});
            return await ServiceBus.Send(new SendInput
            {
                QueueOrTopicName = entityPath,
                ConnectionString = _connectionString,
                Data = data,
                Properties = properties?.Select(kvp => new MessageProperty {Name = kvp.Key, Value = kvp.Value}).ToArray() ?? new MessageProperty[] {}
               
            }, new SendOptions
            {
                CreateQueueOrTopicIfItDoesNotExist = createEntity,
                BodySerializationType = serializationType,
                DestinationType = queueOrTopic,
                TimeoutSeconds = 60,
                ContentType = contentType ?? $"plain/text;charset={encoding.BodyName}"
            }).ConfigureAwait(false);
        }

        private async Task<ReadResult> ReceiveMessage(string entityName, bool useCached = true, QueueOrSubscription queueOrSubscription = QueueOrSubscription.Queue, string subscriptionName = null, bool createEntity = true, BodySerializationType serializationType = BodySerializationType.String, Encoding predefinedEncoding = null)
        {
            predefinedEncoding = predefinedEncoding ?? Encoding.UTF8;
            entitiesToDelete.Add(new EntitySpec { EntityType = queueOrSubscription == QueueOrSubscription.Queue ? QueueOrTopic.Queue : QueueOrTopic.Topic, Name = entityName });
            return await ServiceBus.Read(new ReadInput
            {
                QueueOrTopicName = entityName,
                ConnectionString = _connectionString,
                SourceType = queueOrSubscription,
                SubscriptionName = subscriptionName,
            }, new ReadOptions
            {
                BodySerializationType = serializationType,
                DefaultEncoding = MessageEncoding.Other,
                EncodingName = predefinedEncoding.BodyName,
                CreateQueueOrSubscriptionIfItDoesNotExist = createEntity,
                UseCachedConnection = useCached,
                TimeoutSeconds = 60,
            });
        }

        [TestCase(BodySerializationType.String, "utf-8")]
        [TestCase(BodySerializationType.Stream, "utf-8")]
        [TestCase(BodySerializationType.Stream, "utf-16")]
        [TestCase(BodySerializationType.ByteArray, "utf-8")]
        [TestCase(BodySerializationType.ByteArray, "utf-16")]
        public async Task ShouldSendAndReceiveWithNonExistantQueue(BodySerializationType serializationType, string encoding)
        {
            var data = Path.GetTempFileName();
            _ = await SendMessage(_queueName, data: data, createEntity: true, queueOrTopic: QueueOrTopic.Queue, serializationType: serializationType, encoding:Encoding.GetEncoding(encoding));

            var msg = await ReceiveMessage(_queueName, subscriptionName: _subscriptionName, queueOrSubscription: QueueOrSubscription.Queue, serializationType: serializationType);

            Assert.That(msg.Content, Is.EqualTo(data));
        }

        [Test]
        public async Task ShouldWriteProperties()
        {
            await SendMessage(_queueName, properties: new Dictionary<string, string> {{"property", "propertyValue"}});

            var msg = await ReceiveMessage(_queueName, subscriptionName: _queueName);

            Assert.That(msg.Properties.Count, Is.EqualTo(1));
            var property = msg.Properties.First();
            Assert.That(property.Key, Is.EqualTo("property"));
            Assert.That(property.Value, Is.EqualTo("propertyValue"));
        }

        [Test]
        public async Task ShouldUseContentEncoding()
        {
            var data = Path.GetTempFileName();
            // set wrong encoding in content type
            _ = await SendMessage(_queueName, data: data, createEntity: true, queueOrTopic: QueueOrTopic.Queue, serializationType: BodySerializationType.ByteArray, contentType: "plain/text; charset=ASCII");

            // receive and override
            var msg = await ReceiveMessage(_queueName, subscriptionName: _subscriptionName, queueOrSubscription: QueueOrSubscription.Queue, serializationType: BodySerializationType.ByteArray, predefinedEncoding: Encoding.Unicode);

            Assert.That(msg.Content, Is.EqualTo(data));
        }

        [Test]
        public async Task ShouldUseDefaultEncodingIfContentEncodingNotSpecified()
        {
            var data = Path.GetTempFileName();
            
            await _managementClient.CreateQueueAsync(_queueName);
            // UTF-16 content without content type header
            await (new QueueClient(_connectionString, _queueName)).SendAsync(new Message(ServiceBus.SerializeObject<byte[]>(Encoding.Unicode.GetBytes(data))));

            // receive and override
            var msg = await ReceiveMessage(_queueName, subscriptionName: _subscriptionName, queueOrSubscription: QueueOrSubscription.Queue, serializationType: BodySerializationType.ByteArray, predefinedEncoding: Encoding.Unicode);

            Assert.That(msg.Content, Is.EqualTo(data));
        }

        [Test]
        public async Task ShouldSendAndReceiveWithNonExistantTopic()
        {
            // Receive once so the receiver creates both the topic and subscription
            var emptyMsg = await ReceiveMessage(_queueName, subscriptionName: _subscriptionName, queueOrSubscription: QueueOrSubscription.Queue);
            Assert.That(emptyMsg.ReceivedMessage, Is.False);

            var data = Path.GetTempFileName();
            _ = await SendMessage(_queueName, data: data, createEntity: true, queueOrTopic: QueueOrTopic.Queue);

            var msg = await ReceiveMessage(_queueName, subscriptionName: _subscriptionName, queueOrSubscription: QueueOrSubscription.Queue);

            Assert.That(msg.Content, Is.EqualTo(data));
        }

        [Test]
        public async Task GetQueueInfoReturnsInformation()
        {
            Queue[] queues = { new Queue { QueueName = _queueName2 }, new Queue { QueueName = _queueName3 } };

            var input = new InfoInput
            {
                ConnectionString = _connectionString,
                Queues = queues
            };

            var response = await ServiceBus.GetQueueInfo(input, CancellationToken.None);
            Assert.AreEqual(response.QueueInfos.Count, 2);
        }
    }
}
