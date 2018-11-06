using Amazon.Athena;
using Amazon.Athena.Model;
using Newtonsoft.Json;
using System;
using System.Text;

namespace BAMCIS.LambdaFunctions.AWSAthenaUserMetrics
{
    /// <summary>
    /// Flattened properties from an Athena QueryExecution to record per query metrics
    /// </summary>
    public class AthenaQueryMetric
    {
        #region Public Properties

        /// <summary>
        /// The idof the query execution
        /// </summary>
        public string QueryExecutionId { get; }

        /// <summary>
        /// The database this query was run against
        /// </summary>
        public string Database { get; }

        /// <summary>
        /// The type of SQL statement
        /// </summary>
        public StatementType StatementType { get; }

        /// <summary>
        /// The amount of data scanned
        /// </summary>
        public long DataScannedInBytes { get; }

        /// <summary>
        /// The length of time the query executed
        /// </summary>
        public long EngineExecutionTimeInMillis { get; }

        /// <summary>
        /// The time the query was submitted
        /// </summary>
        public DateTime SubmissionDate { get; }

        /// <summary>
        /// The time the query completed
        /// </summary>
        public DateTime CompletionDate { get; }

        /// <summary>
        /// The current status of the query
        /// </summary>
        public QueryExecutionState Status { get; }

        /// <summary>
        ///  The location in Amazon S3 where your query results are stored, such as s3://path/to/query/bucket/.
        /// </summary>
        public string OutputLocation { get; }

        /// <summary>
        /// The type of KMS encryption used for the output, empty string if none
        /// </summary>
        public string EncryptionConfiguration { get; }

        /// <summary>
        /// The KMS key ARN or ID used to encrypt the output for SSE-KMS or CSE-KMS
        /// </summary>
        public string KmsKey { get; }

        /// <summary>
        /// The base64 encoded UTF8 representation of the SQL query
        /// </summary>
        public string Query { get; }

        /// <summary>
        /// The billing period this query belongs to
        /// </summary>
        public string BillingPeriod { get; }

        #endregion

        #region Constructors

        /// <summary>
        /// Creates an Athena Query Metric from a QueryExecution
        /// </summary>
        /// <param name="queryExecution"></param>
        private AthenaQueryMetric(QueryExecution queryExecution)
        {
            this.QueryExecutionId = queryExecution.QueryExecutionId;
            this.StatementType = queryExecution.StatementType;
            this.DataScannedInBytes = queryExecution.Statistics.DataScannedInBytes;
            this.EngineExecutionTimeInMillis = queryExecution.Statistics.EngineExecutionTimeInMillis;
            this.CompletionDate = queryExecution.Status.CompletionDateTime.ToUniversalTime();
            this.SubmissionDate = queryExecution.Status.SubmissionDateTime.ToUniversalTime();
            this.Status = queryExecution.Status.State;
            this.Database = queryExecution.QueryExecutionContext.Database;
            this.Query = Convert.ToBase64String(Encoding.UTF8.GetBytes(queryExecution.Query));
            this.OutputLocation = queryExecution.ResultConfiguration.OutputLocation;

            if (queryExecution.ResultConfiguration.EncryptionConfiguration != null)
            {
                this.EncryptionConfiguration = queryExecution.ResultConfiguration.EncryptionConfiguration.EncryptionOption.Value ?? String.Empty;
                this.KmsKey = queryExecution.ResultConfiguration.EncryptionConfiguration.KmsKey ?? String.Empty;
            }
            else
            {
                this.EncryptionConfiguration = String.Empty;
                this.KmsKey = String.Empty;
            }

            this.BillingPeriod = queryExecution.Status.SubmissionDateTime.ToUniversalTime().ToString("yyyy-MM-01");
        }
   
        /// <summary>
        /// Creates an Athena Metric
        /// </summary>
        /// <param name="queryExecutionId"></param>
        /// <param name="statementType"></param>
        /// <param name="dataScannedInBytes"></param>
        /// <param name="engineExecutionTimeInMillis"></param>
        /// <param name="completionDate"></param>
        /// <param name="submissionDate"></param>
        /// <param name="status"></param>
        /// <param name="database"></param>
        /// <param name="query"></param>
        /// <param name="outputLocation"></param>
        /// <param name="encryptionConfiguration"></param>
        /// <param name="kmsKey"></param>
        [JsonConstructor()]
        private AthenaQueryMetric(
            string queryExecutionId,
            StatementType statementType,
            long dataScannedInBytes,
            long engineExecutionTimeInMillis,
            DateTime completionDate,
            DateTime submissionDate,
            QueryExecutionState status,
            string database,
            string query,
            string outputLocation,
            string encryptionConfiguration = "",
            string kmsKey = ""
        )
        {
            if (String.IsNullOrEmpty(queryExecutionId))
            {
                throw new ArgumentNullException("queryExecutionId");
            }

            if (String.IsNullOrEmpty(database))
            {
                throw new ArgumentNullException("database");
            }

            if (String.IsNullOrEmpty(query))
            {
                throw new ArgumentNullException("query");
            }

            if (String.IsNullOrEmpty(outputLocation))
            {
                throw new ArgumentNullException("outputLocation");
            }

            if (kmsKey == null)
            {
                kmsKey = String.Empty;
            }

            if (encryptionConfiguration == null)
            {
                encryptionConfiguration = String.Empty;
            }

            if (dataScannedInBytes < 0)
            {
                throw new ArgumentOutOfRangeException("dataScannedInBytes", $"Value cannot be less than zero, {dataScannedInBytes} was provided.");
            }

            if (engineExecutionTimeInMillis < 0)
            {
                throw new ArgumentOutOfRangeException("engineExecutionTimeInMillis", $"Value cannot be less than zero, {engineExecutionTimeInMillis} was provided.");
            }

            this.QueryExecutionId = queryExecutionId;
            this.StatementType = statementType ?? throw new ArgumentNullException("statementType");
            this.DataScannedInBytes = dataScannedInBytes;
            this.EngineExecutionTimeInMillis = engineExecutionTimeInMillis;
            this.CompletionDate = completionDate;
            this.SubmissionDate = submissionDate;
            this.Status = status ?? throw new ArgumentNullException("status");
            this.Database = database;
            this.Query = query;
            this.OutputLocation = outputLocation;
            this.EncryptionConfiguration = encryptionConfiguration;
            this.KmsKey = kmsKey;
        }

        #endregion

        #region Public Methods

        /// <summary>
        /// Creates a new flattened metric from the query execution data
        /// </summary>
        /// <param name="queryExecution"></param>
        /// <returns></returns>
        public static AthenaQueryMetric Build(QueryExecution queryExecution)
        {
            return new AthenaQueryMetric(
                queryExecution.QueryExecutionId,
                queryExecution.StatementType,
                queryExecution.Statistics.DataScannedInBytes,
                queryExecution.Statistics.EngineExecutionTimeInMillis,
                queryExecution.Status.CompletionDateTime.ToUniversalTime(),
                queryExecution.Status.SubmissionDateTime.ToUniversalTime(),
                queryExecution.Status.State,
                queryExecution.QueryExecutionContext.Database,
                Convert.ToBase64String(Encoding.UTF8.GetBytes(queryExecution.Query)),
                queryExecution.ResultConfiguration.OutputLocation,
                queryExecution.ResultConfiguration.EncryptionConfiguration == null ? String.Empty : queryExecution.ResultConfiguration.EncryptionConfiguration.EncryptionOption.Value,
                queryExecution.ResultConfiguration.EncryptionConfiguration == null ? String.Empty : queryExecution.ResultConfiguration.EncryptionConfiguration.KmsKey
            );
        }

        #endregion
    }
}
