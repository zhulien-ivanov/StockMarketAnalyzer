namespace SMDA.DataRetriever
{
    using System;
    using System.Data.SqlClient;
    using System.IO;
    using System.Net;

    public class StockDataRetriever
    {
        private SqlConnection connection;
        private WebClient webClient;
        private string outputDirectoryPath = "../../StockMarketData/";

        private const string ConnectionString =
            @"Server=.\SQLEXPRESS;" +
            "Database=StockMarketAnalyzer;" +
            "Integrated Security=true;";

        private const string CreateTableFormatString =
            "CREATE TABLE {0}" +
            "(" +
                "Date DATE PRIMARY KEY," +
                "OpenPrice NUMERIC(18,6)," +
                "HighPrice NUMERIC(18,6)," +
                "LowPrice NUMERIC(18,6)," +
                "ClosePrice NUMERIC(18,6)," +
                "Volume BIGINT," +
                "AdjClose NUMERIC(18,6)" +
            ");";

        private const string InsertMarketDataFormatString =
            "INSERT INTO [{0}] " +
            "VALUES (@date, @openPrice, @highPrice, @lowPrice, @closePrice, @volume, @adjClose);";

        public StockDataRetriever()
        {
            this.connection = new SqlConnection(ConnectionString);
            this.webClient = new WebClient();
            Directory.CreateDirectory(outputDirectoryPath);
        }

        private void DownloadStockData(string companyName)
        {
            var today = DateTime.Now;

            this.DownloadStockData(companyName, 1, 1, 2000, today.Day, today.Month, today.Year);
        }

        private void DownloadStockData(string companyName, int startDay, int startMonth, int startYear, int endDay, int endMonth, int endYear)
        {
            string URL = string.Format("http://real-chart.finance.yahoo.com/table.csv?s={0}&d={1}&e={2}&f={3}&g=d&a={4}&b={5}&c={6}&ignore=.csv", companyName, endMonth - 1, endDay, endYear, startMonth - 1, startDay, startYear);


            this.webClient.DownloadFile(URL, this.outputDirectoryPath + companyName + ".txt");
        }

        private void InserDataToDatabase(string companyName, SqlConnection connection)
        {
            SqlCommand createTable = new SqlCommand(String.Format(CreateTableFormatString, companyName), connection);
            createTable.ExecuteNonQuery();

            using (var sr = new StreamReader(this.outputDirectoryPath + companyName + ".txt"))
            {
                // Skip the first line which contains the column names
                sr.ReadLine();

                string dataLine = sr.ReadLine();

                while (dataLine != null)
                {
                    var dataLineParts = dataLine.Split(',');

                    DateTime date = DateTime.Parse(dataLineParts[0]);
                    decimal openPrice = decimal.Parse(dataLineParts[1]);
                    decimal highPrice = decimal.Parse(dataLineParts[2]);
                    decimal lowPrice = decimal.Parse(dataLineParts[3]);
                    decimal closePrice = decimal.Parse(dataLineParts[4]);
                    long volume = long.Parse(dataLineParts[5]);
                    decimal adjClose = decimal.Parse(dataLineParts[6]);

                    InsertDataRow(connection, companyName, date, openPrice, highPrice, lowPrice, closePrice, volume, adjClose);

                    dataLine = sr.ReadLine();
                }
            }
        }

        private static void InsertDataRow(SqlConnection connection, string companyName, DateTime date, decimal openPrice, decimal highPrice, decimal lowPrice, decimal closePrice, long volume, decimal adjClose)
        {
            SqlCommand insertPriceData = new SqlCommand(String.Format(InsertMarketDataFormatString, companyName), connection);

            insertPriceData.Parameters.AddWithValue("@date", date);
            insertPriceData.Parameters.AddWithValue("@openPrice", openPrice);
            insertPriceData.Parameters.AddWithValue("@highPrice", highPrice);
            insertPriceData.Parameters.AddWithValue("@lowPrice", lowPrice);
            insertPriceData.Parameters.AddWithValue("@closePrice", closePrice);
            insertPriceData.Parameters.AddWithValue("@volume", volume);
            insertPriceData.Parameters.AddWithValue("@adjClose", adjClose);

            insertPriceData.ExecuteNonQuery();
        }
    }
}