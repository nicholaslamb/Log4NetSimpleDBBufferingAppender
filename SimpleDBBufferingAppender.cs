using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using Amazon.SimpleDB;
using Amazon.SimpleDB.Model;
using log4net;
using log4net.Appender;
using log4net.Core;

namespace Logging
{
    public class SimpleDBBufferingAppender : BufferingAppenderSkeleton
    {
        private int maxItemsPerRequest = 25; // simpledb has max 25 items per batch put request

        protected override void SendBuffer(log4net.Core.LoggingEvent[] events)
        {
            var client = new AmazonSimpleDBClient(); // access and secret keys in web.config

            for (var i = 0; i < (events.Count() / maxItemsPerRequest) + 1; i++)
            {
                try
                {
                    var request = new BatchPutAttributesRequest();

                    foreach (var e in events.Skip(i * maxItemsPerRequest).Take(maxItemsPerRequest))
                    {
                        var batchItem = new ReplaceableItem()
                                            {
                                                ItemName = Guid.NewGuid().ToString()
                                            };

                        batchItem.Attribute.Add(GetAttribute("Thread", e.ThreadName));
                        batchItem.Attribute.Add(GetAttribute("Level", e.Level.Name));
                        batchItem.Attribute.Add(GetAttribute("CustomLevel", GetCustomProperty(e, "CustomLevel")));
                        batchItem.Attribute.Add(GetAttribute("Url", GetCustomProperty(e, "Url")));
                        batchItem.Attribute.Add(GetAttribute("Machine", GetCustomProperty(e, "Machine")));
                        batchItem.Attribute.Add(GetAttribute("Product", GetCustomProperty(e, "Product")));
                        batchItem.Attribute.Add(GetAttribute("UserId", GetCustomProperty(e, "UserId")));
                        batchItem.Attribute.Add(GetAttribute("UserName", GetCustomProperty(e, "UserName")));
                        batchItem.Attribute.Add(GetAttribute("TimeStamp", e.TimeStamp.ToUniversalTime().ToString("o")));
                        batchItem.Attribute.Add(GetAttribute("Message", e.RenderedMessage));
                        batchItem.Attribute.Add(GetAttribute("FormattedMessage", GetCustomProperty(e, "FormattedMessage")));
                        batchItem.Attribute.Add(GetAttribute("StackTrace", e.GetExceptionString()));

                        request.Item.Add(batchItem);
                    }

					// Assumes Domain has already been created
                    if(!string.IsNullOrEmpty(ConfigurationManager.AppSettings["SimpleDBLogName"]))
                        request.DomainName = ConfigurationManager.AppSettings["SimpleDBLogName"];
                    else
                        request.DomainName = "Log";

                    client.BatchPutAttributes(request);
                }
                finally
                {
                    
                }
            }
        }

        private ReplaceableAttribute GetAttribute(string name, string value)
        {
            return new ReplaceableAttribute()
                       {
                           Replace = true,
                           Name = name,
                           Value = value
                       };
        }

        private string GetCustomProperty(LoggingEvent e, string property)
        {
            if (e != null && e.Properties[property] != null)
                return e.Properties[property].ToString();
            else
                return string.Empty;
        }
    }
}
