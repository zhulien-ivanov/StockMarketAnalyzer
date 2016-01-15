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
    }
}