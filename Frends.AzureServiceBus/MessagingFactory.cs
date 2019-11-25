using Microsoft.Azure.ServiceBus;
using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Net;
using Microsoft.Azure.ServiceBus.Core;

namespace Frends.AzureServiceBus
{
    /// <summary>
    /// Class handles clients for the service bus. Enables cached connections to the service bus.
    /// </summary>
    public sealed class ServiceBusMessagingFactory : IDisposable
    {
        private static readonly Lazy<ServiceBusMessagingFactory> instanceHolder = new Lazy<ServiceBusMessagingFactory>(() => new ServiceBusMessagingFactory());
        /// <summary>
        /// The ServiceBusMessagingFactory singleton instance
        /// </summary>
        public static ServiceBusMessagingFactory Instance
        {
            get { return instanceHolder.Value; }
        }

        private static readonly object factoryLock = new Object();
        private readonly ConcurrentDictionary<string, ServiceBusConnection> _connections = new ConcurrentDictionary<string, ServiceBusConnection>();

        private ServiceBusMessagingFactory()
        {

        }


        /// <summary>
        /// Create message receiver for the given connection string and entity path
        /// </summary>
        /// <param name="connectionString">Connection string</param>
        /// <param name="path">Name of the queue</param>
        /// <param name="timeout">TimeoutSeconds</param>
        /// <returns></returns>
        public MessageReceiver GetMessageReceiver(string connectionString, string path, TimeSpan timeout)
        {
            var receiver = new MessageReceiver(GetCachedMessagingFactory(connectionString, timeout), path, receiveMode:ReceiveMode.ReceiveAndDelete);

            return receiver;
        }

        /// <summary>
        /// Create a message sender for the given connection string and entity path
        /// </summary>
        /// <param name="connectionString"></param>
        /// <param name="path"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public MessageSender GetMessageSender(string connectionString, string path, TimeSpan timeout)
        {
            return new MessageSender(GetCachedMessagingFactory(connectionString, timeout), path);
        }

        private ServiceBusConnection GetCachedMessagingFactory(string connectionString, TimeSpan timeout)
        {
            string key = timeout.TotalSeconds + "-" + connectionString;

            if (!_connections.ContainsKey(key))
            {
                lock (factoryLock) // TODO: change double check
                {
                    if (!_connections.ContainsKey(key))
                    {
                        _connections.TryAdd(key, CreateConnectionWithTimeout(connectionString, timeout));
                    }
                }
            }

            return _connections[key];
        }

        /// <summary>
        /// Create new client for servicebus connection. This method is slow!
        /// </summary>
        /// <param name="connectionString">Connection string</param>
        /// <param name="operationTimeoutForClients">Operation timeout for clients</param>
        /// <returns>Object that can handle messaging to the service bus</returns>
        internal static ServiceBusConnection CreateConnectionWithTimeout(string connectionString, TimeSpan operationTimeoutForClients)
        {
            var connBuilder = new ServiceBusConnectionStringBuilder(connectionString)
            {
                OperationTimeout = operationTimeoutForClients
            };

            var connection = new ServiceBusConnection(connBuilder) {RetryPolicy = RetryPolicy.Default};

            return connection;
        }


        #region IDisposable Support
        private bool _disposedValue = false; // To detect redundant calls

        private void Dispose(bool disposing)
        {
            if (!_disposedValue)
            {
                var factoriesToClose = _connections.ToList();
                _connections.Clear();

                if (disposing)
                {
                    // TODO: dispose managed state (managed objects).
                }

                foreach (var item in factoriesToClose)
                {
                    try
                    {
                        item.Value.CloseAsync().Wait();
                    }
                    catch (Exception ex)
                    {
                        Trace.TraceError("Error when aborting messaging factory connection " + ex);
                    }
                }

                _disposedValue = true;
            }
        }

        // TODO: override a finalizer only if Dispose(bool disposing) above has code to free unmanaged resources.
        /// <summary>Allows an object to try to free resources and perform other cleanup operations before it is reclaimed by garbage collection.</summary>
        //~ServiceBusMessagingFactory()
        //{
        //    // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
        //    //Dispose(false);
        //}

        /// <summary>
        /// Dispose of the MessagingFactory and close all the cached connections
        /// </summary>
        public void Dispose()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(true);

            //GC.SuppressFinalize(this);
        }
        #endregion

    }
}
