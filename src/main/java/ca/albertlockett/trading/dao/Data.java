package ca.albertlockett.trading.dao;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Map;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.ta4j.core.BaseTimeSeries;
import org.ta4j.core.TimeSeries;

public class Data {

  private static DateTimeFormatter formatter = new DateTimeFormatterBuilder()
      .appendPattern("yyyy-MM-dd[ HH:mm:ss]")
      .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
      .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
      .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
      .toFormatter();

  public TimeSeries fetchBarData(String from, String to, String symbol) {

    TimeSeries series = new BaseTimeSeries.SeriesBuilder()
      .withName(symbol)
      .setConstrained(false)
      .build();

    RestHighLevelClient client = ClientFactory.getInstance().getClient();
    SearchRequest searchRequest = new SearchRequest("bars");
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.query(
      QueryBuilders.boolQuery()
        .filter(QueryBuilders.rangeQuery("date").gt(from).lt(to))
        .must(QueryBuilders.matchQuery("symbol", symbol)));
    searchSourceBuilder.sort(new FieldSortBuilder("date").order(SortOrder.ASC));  

    searchRequest.source(searchSourceBuilder);

    try {
      SearchResponse response = client.search(searchRequest);

      response.getHits().forEach(hit -> {
        Map<String, Object> map = hit.getSourceAsMap();

        ZonedDateTime date = LocalDateTime.parse(map.get("date").toString(), formatter).atZone(ZoneId.of("US/Eastern"));
        Double open = Double.parseDouble(map.get("open").toString());
        Double close = Double.parseDouble(map.get("open").toString());
        Double high = Double.parseDouble(map.get("high").toString());
        Double low = Double.parseDouble(map.get("low").toString());
        Double volume = Double.parseDouble(map.get("volume").toString());
        series.addBar(date, open, high, low, close, volume);
      });
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return series;
  }

}