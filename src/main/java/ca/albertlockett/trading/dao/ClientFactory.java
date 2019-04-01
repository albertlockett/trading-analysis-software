package ca.albertlockett.trading.dao;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

public class ClientFactory {

  private static ClientFactory instance;

  private final RestHighLevelClient client;

  protected ClientFactory() { 
    this.client = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 19200)));
  }

  public synchronized static ClientFactory getInstance() {
    if (ClientFactory.instance == null) {
      ClientFactory.instance = new ClientFactory();
    }
    return ClientFactory.instance;
  }


  public RestHighLevelClient getClient() {
    return this.client;
  }

}