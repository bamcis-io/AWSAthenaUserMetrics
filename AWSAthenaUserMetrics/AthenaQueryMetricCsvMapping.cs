using CsvHelper.Configuration;
using CsvHelper.TypeConversion;

namespace BAMCIS.LambdaFunctions.AWSAthenaUserMetrics
{
    /// <summary>
    /// The class mapper for the Athena Query Metric class
    /// </summary>
    internal sealed class AthenaQueryMetricCsvMapping : ClassMap<AthenaQueryMetric>
    {
        #region Constructors

        /// <summary>
        /// Creates the csv mapping
        /// </summary>
        public AthenaQueryMetricCsvMapping()
        {
            Map(x => x.QueryExecutionId).Index(0);
            Map(x => x.Database).Index(1);
            Map(x => x.StatementType.Value).Name("StatementType").Index(2);
            Map(x => x.DataScannedInBytes).Index(3);
            Map(x => x.EngineExecutionTimeInMillis).Index(4);
            Map(x => x.SubmissionDate).Index(5).TypeConverterOption.DateTimeStyles(System.Globalization.DateTimeStyles.AdjustToUniversal).TypeConverterOption.Format("yyyy-MM-dd HH:mm:ss.fff");
            Map(x => x.CompletionDate).Index(6).TypeConverterOption.DateTimeStyles(System.Globalization.DateTimeStyles.AdjustToUniversal).TypeConverterOption.Format("yyyy-MM-dd HH:mm:ss.fff");
            Map(x => x.Status.Value).Name("Status").Index(7);
            Map(x => x.OutputLocation).Index(8);
            Map(x => x.EncryptionConfiguration).Index(9);
            Map(x => x.KmsKey).Index(10);
            Map(x => x.Query).Index(11);
            Map(x => x.BillingPeriod).Index(12);
        }

        #endregion
    }
}
