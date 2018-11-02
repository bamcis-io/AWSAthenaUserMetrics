using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Xunit;
using Amazon.Lambda.Core;
using Amazon.Lambda.TestUtilities;
using Amazon.Lambda.APIGatewayEvents;

using AWSAthenaUserMetrics;
using BAMCIS.AWSLambda.Common.Events;
using Newtonsoft.Json;
using BAMCIS.LambdaFunctions.AWSAthenaUserMetrics;
using Amazon;
using Parquet;
using System.IO;
using Parquet.Data;
using Parquet.Serialization;

namespace AWSAthenaUserMetrics.Tests
{
    public class EntrypointTests
    {
        private static readonly string MARKER_BUCKET = $"{Environment.UserName}-athena-data";
        private static readonly string MARKER_KEY = $"marker.txt";
        private static readonly string RESULT_BUCKET = $"{Environment.UserName}-athena-data";
        private static readonly string RETRY_BUCKET = $"{Environment.UserName}-athena-data";
        private static readonly string RETRY_KEY = "retry.txt";

        private static readonly string CLOUDWATCH_SCHEDULED_EVENT = @"
{
""version"":""0"",
""id"":""125e7841-c049-462d-86c2-4efa5f64e293"",""detail-type"":""Scheduled Event"",""source"":""aws.events"",
""account"":""415720405880"",
""time"":""2016-12-16T19:55:42Z"",
""region"":""us-east-1"",
""resources"":[
""arn:aws:events:us-east-1:415720405880:rule/BackupTest-GetGetBackups-X2YM3334N4JN""
],
""detail"":{}
}".Trim().Replace("\r", "").Replace("\n", "").Replace("\t", "");

        private static readonly CloudWatchScheduledEvent Event = JsonConvert.DeserializeObject<CloudWatchScheduledEvent>(CLOUDWATCH_SCHEDULED_EVENT);


        public EntrypointTests()
        {
        }

        [Fact]
        public async Task TestExec()
        {
            // ARRANGE

            Entrypoint Entry = new Entrypoint();

            TestLambdaLogger TestLogger = new TestLambdaLogger();
            TestClientContext ClientContext = new TestClientContext();

            ILambdaContext Context = new TestLambdaContext()
            {
                FunctionName = "Common",
                FunctionVersion = "1",
                Logger = TestLogger,
                ClientContext = ClientContext
            };

            Environment.SetEnvironmentVariable("MARKER_BUCKET", MARKER_BUCKET);
            Environment.SetEnvironmentVariable("MARKER_KEY", MARKER_KEY);
            Environment.SetEnvironmentVariable("RESULT_BUCKET", RESULT_BUCKET);

            // ACT

            await Entry.ExecAsync(Event, Context);


            // ASSERT
        }

        [Fact]
        public async Task TestRetry()
        {
            // ARRANGE

            Entrypoint Entry = new Entrypoint();

            TestLambdaLogger TestLogger = new TestLambdaLogger();
            TestClientContext ClientContext = new TestClientContext();

            ILambdaContext Context = new TestLambdaContext()
            {
                FunctionName = "Common",
                FunctionVersion = "1",
                Logger = TestLogger,
                ClientContext = ClientContext
            };

            Environment.SetEnvironmentVariable("MARKER_BUCKET", MARKER_BUCKET);
            Environment.SetEnvironmentVariable("MARKER_KEY", MARKER_KEY);
            Environment.SetEnvironmentVariable("RESULT_BUCKET", RESULT_BUCKET);
            Environment.SetEnvironmentVariable("RETRY_BUCKET", RETRY_BUCKET);
            Environment.SetEnvironmentVariable("RETRY_KEY", RETRY_KEY);

            // ACT

            await Entry.RetryAsync(Event, Context);


            // ASSERT
        }
    }
}
