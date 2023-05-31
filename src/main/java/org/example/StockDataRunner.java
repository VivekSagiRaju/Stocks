package org.example;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

public class StockDataRunner {
    public static void main(String[] args) {
        String apiUrl = "https://api.stockdata.org/v1/data/eod";
        String apiToken = "5dGDofhWvp2p3bSx5xkeR4RqNfIdADmGkkggGGOg";
        String symbol = "AAPL";
        String interval = "day";
        String sortOrder = "desc";
        String dateFrom = "2022-01-01";
        String dateTo = "2023-12-31";
        String format = "json";

        String url = apiUrl + "?api_token=" + apiToken + "&symbols=" + symbol + "&interval=" + interval +
                "&sort=" + sortOrder + "&date_from=" + dateFrom + "&date_to=" + dateTo + "&format=" + format;

        try {

            Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/postgres", "postgres", "Vickyraj@7");

            URL apurl = new URL(url);
            HttpURLConnection connect = (HttpURLConnection) apurl.openConnection();
            connect.setRequestMethod("GET");

            int responseCode = connect.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(connect.getInputStream()));
                StringBuilder response = new StringBuilder();
                String line;

                while ((line = reader.readLine()) != null) {
                    response.append(line);
                }
                reader.close();

                String data = response.toString();

                // Parse
                JsonParser jsonParser = new JsonParser();
                JsonObject jsonObject = jsonParser.parse(data).getAsJsonObject();

                JsonObject metaData = jsonObject.getAsJsonObject("meta");
                String stock = metaData.get("ticker").getAsString();

                JsonArray jsonData = jsonObject.getAsJsonArray("data");
                for (JsonElement element : jsonData) {
                    JsonObject stockData = element.getAsJsonObject();
                    String dateString = stockData.get("date").getAsString();
                    Timestamp date = Timestamp.valueOf(dateString.replace("T", " ").replace("Z", ""));

                    double open = stockData.get("open").getAsDouble();
                    double high = stockData.get("high").getAsDouble();
                    double low = stockData.get("low").getAsDouble();
                    double close = stockData.get("close").getAsDouble();
                    long volume = stockData.get("volume").getAsLong();

                    // Prepare the SQL statement
                    String sql = "INSERT INTO public.\"Stock_Historical_Data\"(\n" +
                            "\tstock, date, open, high, low, close, volume)\n" +
                            "\tVALUES (?, ?, ?, ?, ?, ?, ?);";
                    PreparedStatement statement = connection.prepareStatement(sql);
                    statement.setString(1, stock);
                    statement.setTimestamp(2, date);
                    statement.setDouble(3, open);
                    statement.setDouble(4, high);
                    statement.setDouble(5, low);
                    statement.setDouble(6, close);
                    statement.setLong(7, volume);

                    statement.executeUpdate();
                }

                System.out.println("Data inserted successfully.");
            } else {
                System.out.println("Failed to fetch data. Response Code: " + responseCode);
            }

            connect.disconnect();
        } catch (IOException | SQLException e) {
            e.printStackTrace();
        }
    }
}
