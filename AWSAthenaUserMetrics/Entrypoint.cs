using Amazon;
using Amazon.Athena;
using Amazon.Athena.Model;
using Amazon.Lambda.Core;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Transfer;
using BAMCIS.AWSLambda.Common;
using BAMCIS.AWSLambda.Common.Events;
using CsvHelper;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Threading.Tasks;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace BAMCIS.LambdaFunctions.AWSAthenaUserMetrics
{
    public class Entrypoint
    {
        #region Private Fields

        private static IAmazonS3 _S3Client;
        private static IAmazonAthena _AthenaClient;

        private static readonly string MARKER_BUCKET = "MARKER_BUCKET";
        private static readonly string MARKER_KEY = "MARKER_KEY";
        private static readonly string RESULT_BUCKET = "RESULT_BUCKET";
        private static readonly string RETRY_BUCKET = "RETRY_BUCKET";
        private static readonly string RETRY_KEY = "RETRY_KEY";

        #endregion

        #region Constructors

        static Entrypoint()
        {
#if DEBUG
            AWSConfigs.AWSProfileName = $"{Environment.UserName}-dev";
            AWSConfigs.AWSProfilesLocation = $"{Environment.GetEnvironmentVariable("UserProfile")}\\.aws\\credentials";
            StoredProfileAWSCredentials Creds = new StoredProfileAWSCredentials();

            _S3Client = new AmazonS3Client(Creds);
            _AthenaClient = new AmazonAthenaClient(Creds);
#else

            _S3Client = new AmazonS3Client();
            _AthenaClient = new AmazonAthenaClient();
#endif
        }

        /// <summary>
        /// Default constructor that Lambda will invoke.
        /// </summary>
        public Entrypoint()
        {
        }

        #endregion

        #region Public Methods

        /// <summary>
        /// Processes the execution Ids that need be retried because they weren't finished or cancelled
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public async Task RetryAsync(CloudWatchScheduledEvent request, ILambdaContext context)
        {
            context.LogInfo($"Received scheduled event for retries:\n{JsonConvert.SerializeObject(request)}");

            List<string> RetryIds = await GetRetryFileAsync(Environment.GetEnvironmentVariable(RETRY_BUCKET), Environment.GetEnvironmentVariable(RETRY_KEY), context);

            List<string> RemainingIds = new List<string>();

            if (RetryIds != null && RetryIds.Any())
            {
                int Counter = 0;

                foreach (List<string> Chunk in ChunkList<string>(RetryIds, 50))
                {
                    BatchGetQueryExecutionRequest BatchRequest = new BatchGetQueryExecutionRequest()
                    {
                        QueryExecutionIds = Chunk
                    };

                    BatchGetQueryExecutionResponse BatchResponse = await _AthenaClient.BatchGetQueryExecutionAsync(BatchRequest);

                    if (BatchResponse == null)
                    {
                        context.LogError($"The batch response was null, this shouldn't happen.");
                        return;
                    }

                    // Make sure we received a good status code
                    if (BatchResponse.HttpStatusCode != HttpStatusCode.OK)
                    {
                        context.LogError($"The batch request did not return a success status code: {(int)BatchResponse.HttpStatusCode}.");
                        return;
                    }

                    // Make sure we actually received data back
                    if (BatchResponse.QueryExecutions == null || !BatchResponse.QueryExecutions.Any())
                    {
                        context.LogError($"The batch response did not contain any query executions.");
                    }
                    else
                    {
                        // These are all the transformed records
                        IEnumerable<AthenaQueryMetric> Records = BatchResponse.QueryExecutions.Select(x => new AthenaQueryMetric(x));

                        // These are the queries that either succeeded or were cancelled and are done
                        List<AthenaQueryMetric> FinishedQueries = Records.Where(x => x.Status == QueryExecutionState.SUCCEEDED.Value || x.Status == QueryExecutionState.CANCELLED.Value).ToList();

                        // These are the queries that are still running or are queued
                        List<string> NotFinishedQueries = Records.Where(x => x.Status == QueryExecutionState.RUNNING.Value || x.Status == QueryExecutionState.QUEUED.Value).Select(x => x.QueryExecutionId).ToList();

                        if (NotFinishedQueries.Any())
                        {
                            RemainingIds.AddRange(NotFinishedQueries);
                        }

                        // Nothing to write, so skip to next iteration
                        if (!FinishedQueries.Any())
                        {
                            context.LogInfo("No successful queries found in this list.");
                            continue;
                        }
                        else
                        {
                            Counter += FinishedQueries.Count;
                            await WriteDataAsync(FinishedQueries, Environment.GetEnvironmentVariable(RESULT_BUCKET), context);

                        }
                    }
                }

                context.LogInfo($"Finished pulling query execution data and writing to S3. Wrote {Counter} records.");

                if (RemainingIds.Count < RetryIds.Count)
                {
                    context.LogInfo("Updating retry file.");
                    await SetRetryFileAsync(Environment.GetEnvironmentVariable(RETRY_BUCKET), Environment.GetEnvironmentVariable(RETRY_KEY), RemainingIds, context);
                    context.LogInfo("Finished updating retry file.");
                }
                else
                {
                    context.LogInfo("No updates need to made to the retry file.");
                }

            }
            else
            {
                context.LogInfo("No ids in the retry file.");
            }
        }

        /// <summary>
        /// A Lambda function to respond to HTTP Get methods from API Gateway
        /// </summary>
        /// <param name="request"></param>
        /// <returns>The list of blogs</returns>
        public async Task ExecAsync(CloudWatchScheduledEvent request, ILambdaContext context)
        {
            context.LogInfo($"Received scheduled event:\n{JsonConvert.SerializeObject(request)}");

            // The list request for the query execution Ids
            ListQueryExecutionsRequest ListRequest = new ListQueryExecutionsRequest();

            // Retrieve the last query execution id that was processed, i.e. the most recent one
            // the last time it ran
            string LastReadQueryExecutionId = await GetLastQueryExecutionIdAsync(Environment.GetEnvironmentVariable(MARKER_BUCKET), Environment.GetEnvironmentVariable(MARKER_KEY));

            context.LogInfo($"Previous run last processed query execution id: {LastReadQueryExecutionId}.");

            // Track whether we're done in the do/while loop
            bool Finished = false;

            // Track whether this is the first time through the loop so we
            // can grab the first execution id
            bool FirstLoop = true;

            // This will be considered the most recent query, grab it here
            // and we'll write it at the end when everything's done and we're sure this all succeeded
            string NewLastQueryExecutionId = String.Empty;

            // This will count the number of successful queries written to S3 in total
            int Counter = 0;

            do
            {
                // Get the same list we got above again
                ListQueryExecutionsResponse ListResponse = await _AthenaClient.ListQueryExecutionsAsync(ListRequest);

                if (ListResponse.HttpStatusCode != HttpStatusCode.OK)
                {
                    context.LogError($"The list request did not return a success status code: {(int)ListResponse.HttpStatusCode}.");
                    return;
                }

                // If the list response is null of doesn't have query execution ids, stop processing
                if (ListResponse == null || ListResponse.QueryExecutionIds == null || !ListResponse.QueryExecutionIds.Any())
                {
                    context.LogWarning("The list response was null or the query execution Ids were null or empty.");
                    break;
                }

                // If it's the first loop
                if (FirstLoop)
                {
                    NewLastQueryExecutionId = ListResponse.QueryExecutionIds.First();
                    context.LogInfo($"The new last processed query execution id will be: {NewLastQueryExecutionId}.");
                    FirstLoop = false;

                    if (LastReadQueryExecutionId == NewLastQueryExecutionId)
                    {
                        context.LogInfo("No new query execution ids.");
                        break;
                    }
                }

                // Batch get the query executions based on ids
                BatchGetQueryExecutionRequest BatchRequest = new BatchGetQueryExecutionRequest()
                {
                    QueryExecutionIds = ListResponse.QueryExecutionIds
                };

                // If any of the ids match the last read id, then we're done listing ids since
                // we've gotten back to the start of the last run
                if (ListResponse.QueryExecutionIds.Any(x => x.Equals(LastReadQueryExecutionId)))
                {
                    BatchRequest.QueryExecutionIds = BatchRequest.QueryExecutionIds.Where(x => !x.Equals(LastReadQueryExecutionId)).ToList();
                    Finished = true;
                }

                // Make sure there were ids in the request
                if (BatchRequest.QueryExecutionIds.Any())
                {
                    // Get query execution details
                    BatchGetQueryExecutionResponse BatchResponse = await _AthenaClient.BatchGetQueryExecutionAsync(BatchRequest);

                    if (BatchResponse == null)
                    {
                        context.LogError($"The batch response was null, this shouldn't happen.");
                        return;
                    }

                    // Make sure we received a good status code
                    if (BatchResponse.HttpStatusCode != HttpStatusCode.OK)
                    {
                        context.LogError($"The batch request did not return a success status code: {(int)BatchResponse.HttpStatusCode}.");
                        return;
                    }

                    // Make sure we actually received data back
                    if (BatchResponse.QueryExecutions == null || !BatchResponse.QueryExecutions.Any())
                    {
                        context.LogError($"The batch response did not contain any query executions.");
                    }
                    else
                    {
                        // These are all the transformed records
                        IEnumerable<AthenaQueryMetric> Records = BatchResponse.QueryExecutions.Select(x => new AthenaQueryMetric(x));

                        // These are the queries that either succeeded or were cancelled and are done
                        List<AthenaQueryMetric> FinishedQueries = Records.Where(x => x.Status == QueryExecutionState.SUCCEEDED.Value || x.Status == QueryExecutionState.CANCELLED.Value).ToList();

                        // These are the queries that are still running or are queued
                        List<string> NotFinishedQueries = Records.Where(x => x.Status == QueryExecutionState.RUNNING.Value || x.Status == QueryExecutionState.QUEUED.Value).Select(x => x.QueryExecutionId).ToList();

                        // This block updates the retry list stored in S3
                        if (NotFinishedQueries.Any())
                        {
                            context.LogInfo("Adding to the not finished queries list.");

                            PutObjectResponse Response = await UpdateRetryFileAsync(
                                Environment.GetEnvironmentVariable(RETRY_BUCKET),
                                Environment.GetEnvironmentVariable(RETRY_KEY),
                                NotFinishedQueries,
                                context
                            );

                            if (Response.HttpStatusCode != HttpStatusCode.OK)
                            {
                                context.LogError($"Failed to upload retry file with status code: {(int)Response.HttpStatusCode}. Request Id: {Response.ResponseMetadata.RequestId}.");
                            }
                        }

                        // Nothing to write, so skip to next iteration
                        if (!FinishedQueries.Any())
                        {
                            context.LogInfo("No successful queries found in this list.");
                            continue;
                        }

                        // Add the finished queries to the total count
                        Counter += FinishedQueries.Count;

                        // Write the finished query data to S3
                        await WriteDataAsync(FinishedQueries, Environment.GetEnvironmentVariable(RESULT_BUCKET), context);
                    }
                }

                if (!String.IsNullOrEmpty(ListResponse.NextToken))
                {
                    ListRequest.NextToken = ListResponse.NextToken;
                }
                else
                {
                    ListRequest.NextToken = String.Empty;
                }

            } while (!String.IsNullOrEmpty(ListRequest.NextToken) && !Finished);

            context.LogInfo($"Finished pulling query execution data and writing to S3. Wrote {Counter} records.");

            // Only update the new last query id if it's not empty, which might happen if there are no Ids in the first list
            // response, of if the new is the same as the old, meaning we didn't process any new queries
            if (!String.IsNullOrEmpty(NewLastQueryExecutionId) && NewLastQueryExecutionId != LastReadQueryExecutionId)
            {
                await SetLastQueryExecutionIdAsync(Environment.GetEnvironmentVariable(MARKER_BUCKET), Environment.GetEnvironmentVariable(MARKER_KEY), NewLastQueryExecutionId);
                context.LogInfo($"Completed updating marker to {NewLastQueryExecutionId}.");
            }
            else
            {
                context.LogInfo($"No new query executions, not updating marker.");
            }

            context.LogInfo("Function complete.");
        }

        #endregion

        #region Private Methods

        /// <summary>
        /// Gets the last read query execution id stored in an s3 object
        /// </summary>
        /// <param name="bucket"></param>
        /// <param name="key"></param>
        /// <returns></returns>
        private static async Task<string> GetLastQueryExecutionIdAsync(string bucket, string key)
        {
            string LastQueryExecutionId = String.Empty;

            try
            {
                using (Stream Content = await _S3Client.GetObjectStreamAsync(bucket, key, null))
                {
                    using (StreamReader Reader = new StreamReader(Content))
                    {
                        LastQueryExecutionId = Reader.ReadToEnd();
                    }
                }
            }
            catch (Exception)
            {

            }

            return LastQueryExecutionId;
        }

        /// <summary>
        /// Updates the contents of the marker file with the latest query execution id
        /// </summary>
        /// <param name="bucket"></param>
        /// <param name="key"></param>
        /// <param name="queryExecutionId"></param>
        /// <returns></returns>
        private static async Task SetLastQueryExecutionIdAsync(string bucket, string key, string queryExecutionId)
        {
            PutObjectRequest Request = new PutObjectRequest()
            {
                BucketName = bucket,
                Key = key,
                ContentType = "text/plain",
                ContentBody = queryExecutionId
            };

            await _S3Client.PutObjectAsync(Request);
        }

        /// <summary>
        /// Updates the file in S3 that contain the list of Ids that need to be retried because they weren't finished, the provided
        /// Ids are appended to the existing content
        /// </summary>
        /// <param name="bucket"></param>
        /// <param name="key"></param>
        /// <param name="ids"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        private static async Task<PutObjectResponse> UpdateRetryFileAsync(string bucket, string key, List<string> ids, ILambdaContext context)
        {
            IEnumerable<string> Contents = await GetRetryFileAsync(bucket, key, context);

            if (Contents != null && Contents.Any())
            {
                ids.InsertRange(0, Contents);
            }

            return await SetRetryFileAsync(bucket, key, ids, context);
        }

        /// <summary>
        /// Sets the contents of the file in S3 that contain the list of Ids that need to be retried because they weren't finished
        /// </summary>
        /// <param name="bucket"></param>
        /// <param name="key"></param>
        /// <param name="ids"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        private static async Task<PutObjectResponse> SetRetryFileAsync(string bucket, string key, List<string> ids, ILambdaContext context)
        {
            PutObjectRequest PutRequest = new PutObjectRequest()
            {
                BucketName = Environment.GetEnvironmentVariable(RETRY_BUCKET),
                Key = Environment.GetEnvironmentVariable(RETRY_KEY),
                ContentBody = String.Join("\n", ids),
                ContentType = "text/plain"
            };

            return await _S3Client.PutObjectAsync(PutRequest);
        }

        /// <summary>
        /// Gets the contents of the retry file
        /// </summary>
        /// <param name="bucket"></param>
        /// <param name="key"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        private static async Task<List<string>> GetRetryFileAsync(string bucket, string key, ILambdaContext context)
        {
            try
            {
                GetObjectRequest GetRequest = new GetObjectRequest()
                {
                    BucketName = bucket,
                    Key = key
                };

                GetObjectResponse Response = await _S3Client.GetObjectAsync(GetRequest);

                using (Stream ResponseStream = Response.ResponseStream)
                {
                    using (StreamReader Reader = new StreamReader(ResponseStream))
                    {
                        return Reader.ReadToEnd().Split("\n").Select(x => x.Trim()).ToList();
                    }
                }
            }
            catch (Exception e)
            {
                context.LogError("It's likely the retry file does not exist.", e);
            }

            return Enumerable.Empty<string>().ToList();
        }

        /// <summary>
        /// Chunks an IEnumerable into multiple lists of a specified size
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="input"></param>
        /// <param name="chunkSize"></param>
        /// <returns></returns>
        private static IEnumerable<List<T>> ChunkList<T>(IEnumerable<T> input, int chunkSize)
        {
            if (chunkSize <= 0)
            {
                throw new ArgumentOutOfRangeException("chunkSize", "The chunk size must be greater than 0.");
            }

            if (input == null)
            {
                throw new ArgumentNullException("input");
            }

            if (input.Any())
            {
                IEnumerator<T> Enumerator = input.GetEnumerator();
                List<T> ReturnList = new List<T>(chunkSize);
                int Counter = 1;

                while (Enumerator.MoveNext())
                {
                    if (Counter >= chunkSize)
                    {
                        yield return ReturnList;
                        ReturnList = new List<T>();
                        Counter = 1;
                    }

                    ReturnList.Add(Enumerator.Current);
                    Counter++;
                }

                yield return ReturnList;
            }
        }

        /// <summary>
        /// Uploads the finished query data to S3
        /// </summary>
        /// <param name="finishedQueries"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        private static async Task WriteDataAsync(IEnumerable<AthenaQueryMetric> finishedQueries, string bucket, ILambdaContext context)
        {

            foreach (IGrouping<string, AthenaQueryMetric> Group in finishedQueries.GroupBy(x => x.BillingPeriod))
            {
                // Maintains all of the disposables that need to be disposed of at the end, but
                // not before the streams have been completely read and uploaded, otherwise, it causes 
                // a race condition if we use a using block where the streams will close before the 
                // transfer utility has finished the upload
                List<IDisposable> Disposables = new List<IDisposable>();



                try
                {
                    // The memory stream the compressed stream will be written into
                    MemoryStream MStreamOut = new MemoryStream();

                    // The Gzip Stream only writes its file footer 10 byte data when the stream is closed
                    // Calling dispose via the using block flushes and closes the stream first causing the 
                    // the footer data to be written out to the memory stream. The third parameter "true"
                    // allows the memorystream to still access the gzip stream data, otherwise when trying to
                    // upload the stream via the transfer utility, it will cause an exception that the stream
                    // is closed
                    using (GZipStream Gzip = new GZipStream(MStreamOut, CompressionLevel.Optimal, true))
                    {
                        TextWriter TWriter = new StreamWriter(Gzip);
                        CsvWriter Writer = new CsvWriter(TWriter);

                        Writer.Configuration.RegisterClassMap<AthenaQueryMetricCsvMapping>();

                        Disposables.Add(Writer);
                        Disposables.Add(TWriter);
                        Disposables.Add(MStreamOut);

                        Writer.WriteHeader<AthenaQueryMetric>();
                        Writer.NextRecord(); // Advance the writer to the next line before
                                             // writing the records
                        Writer.WriteRecords<AthenaQueryMetric>(finishedQueries);

                        // Make sure to flush all of the data to the stream
                        Writer.Flush();
                        TWriter.Flush();
                    }


                    /*
                    Schema PSchema = SchemaReflector.Reflect<AthenaQueryMetric>();

                    ParquetConvert.Serialize<AthenaQueryMetric>(FinishedQueries, MStreamOut, PSchema);
                    */

                    // Make the transfer utility request to post the query data csv content
                    TransferUtilityUploadRequest Request = new TransferUtilityUploadRequest()
                    {
                        BucketName = bucket,
                        Key = $"data/billingperiod={Group.Key}/{finishedQueries.First().QueryExecutionId}_{finishedQueries.Last().QueryExecutionId}.csv.gz",
                        InputStream = MStreamOut,
                        AutoResetStreamPosition = true,
                        AutoCloseStream = true,
                        ContentType = "text/csv"
                    };

                    using (TransferUtility XferUtil = new TransferUtility(_S3Client))
                    {
                        try
                        {
                            context.LogInfo($"Starting file upload of {MStreamOut.Length} bytes: ${Request.Key}.");
                            // Make the upload 
                            await XferUtil.UploadAsync(Request);
                            context.LogInfo($"Finished upload of {Request.Key}.");
                        }
                        catch (Exception e)
                        {
                            context.LogError(e);
                        }
                    }
                }
                catch (Exception e)
                {
                    context.LogError(e);
                }
                finally
                {
                    // Dispose all of the streams and writers used to
                    // write the CSV content, we need to dispose of these here
                    // so the memory stream doesn't get closed by disposing
                    // of the writers too early, which will cause the transfer utility
                    // to fail the upload
                    foreach (IDisposable Item in Disposables)
                    {
                        try
                        {
                            Item.Dispose();
                        }
                        catch { }
                    }

                    // Make sure memory is cleaned up
                    GC.Collect();
                    GC.WaitForPendingFinalizers();
                }
            }
        }

        #endregion
    }
}
