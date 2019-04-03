package ca.albertlockett.trading.jobs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

public class LoadRawData {

  private Integer writeThreshold;
  private String indexName;
  private RestHighLevelClient client;
  private String rawDataFolderName;

  public LoadRawData(Integer writeThreshold, RestHighLevelClient client, String rawDataFolderName, String indexName) {
    this.writeThreshold = writeThreshold;
    this.client = client;
    this.rawDataFolderName = rawDataFolderName;
    this.indexName = indexName;
  }

  public static void main(String[] args) {
    final RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 19200)));
    LoadRawData job = new LoadRawData(1000, client, "./data/raw/price_eod_bars/", "bars");

    // init the index
    try {
      GetIndexRequest getIndexRequest = new GetIndexRequest().indices("bars");
      if (client.indices().exists(getIndexRequest)) {
        System.err.println("index 'bars' already exists");
        System.exit(-1);
        return;
      }
      
      ClassLoader classLoader = job.getClass().getClassLoader();
      File file = new File(classLoader.getResource("bars-index-mapping.json").getFile());
      BufferedReader reader = new BufferedReader(new FileReader(file));
      StringBuffer fileContents = new StringBuffer();
      while (reader.ready()) fileContents.append(reader.readLine());
      reader.close();

      CreateIndexRequest request = new CreateIndexRequest("bars");
      request.source(fileContents.toString(), XContentType.JSON);
      client.indices().create(request);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      System.exit(-1);
      return;
    }

    job.run();
    System.exit(0);
  }

  public void run() {
    File rawDataFolder = new File(this.rawDataFolderName);
    List<File> files = Arrays.asList(rawDataFolder.listFiles());
    files.parallelStream().forEach(this::writeData);
  }

  private void writeData(File file) {

    String fileName = file.getName();
    String symbol = fileName.split("\\.")[0];
    System.out.println("writing data for " + symbol);

    try (FileInputStream fis = new FileInputStream(file)) {
      BufferedReader reader = new BufferedReader(new InputStreamReader(fis));

      // read the first 20 lines and discard, that's the header info
      for (int i = 0; i < 20; i++) { reader.readLine(); }

      List<Map<String, String>> bars = new ArrayList<>();
      while (reader.ready()) {
        Map<String, String> bar = this.readBarData(reader);
        bar.put("symbol", symbol);
        bars.add(bar);

        if (bars.size() >= writeThreshold) {
          this.exportBars(bars);
          bars.clear();
        }
      }

    } catch (Exception e) {
      System.out.println("ERROR HAPPEN ON SYMBOL" + symbol);
      // throw new RuntimeException(e);
    }

  }


  private Map<String, String> readBarData(BufferedReader reader) throws IOException {
    Map<String, String> bar = new HashMap<>();
    reader.readLine(); // read the line with the open [
    bar.put("date", reader.readLine().trim().replace(",", "").replace("\"", "").replace("\"", ""));
    bar.put("open", reader.readLine().trim().replace(",", ""));
    bar.put("high", reader.readLine().trim().replace(",", ""));
    bar.put("low", reader.readLine().trim().replace(",", ""));
    bar.put("close", reader.readLine().trim().replace(",", ""));
    bar.put("volume", reader.readLine().trim().replace(",", ""));
    bar.put("dividend", reader.readLine().trim().replace(",", ""));
    bar.put("split", reader.readLine().trim().replace(",", ""));
    bar.put("adj_open", reader.readLine().trim().replace(",", ""));
    bar.put("adj_high", reader.readLine().trim().replace(",", ""));
    bar.put("adj_low", reader.readLine().trim().replace(",", ""));
    bar.put("adj_close", reader.readLine().trim().replace(",", ""));
    bar.put("adj_volume", reader.readLine().trim().replace(",", ""));
    reader.readLine(); // read the line with the open ]
    return bar;
  }

  private void exportBars(List<Map<String, String>> bars) throws IOException {
    BulkRequest request = new BulkRequest();
    bars.forEach(bar -> {
      request.add(new IndexRequest(this.indexName, "bar").source(bar));
    });
    BulkResponse response = this.client.bulk(request);
  }

}