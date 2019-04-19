package ca.albertlockett.trading.dao;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
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
    Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));
    SearchRequest searchRequest = new SearchRequest("bars");
    searchRequest.scroll(scroll);

    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.query(
      QueryBuilders.boolQuery()
        .filter(QueryBuilders.rangeQuery("date").gt(from).lt(to))
        .must(QueryBuilders.matchQuery("symbol", symbol)));
    searchSourceBuilder.sort(new FieldSortBuilder("date").order(SortOrder.ASC));  
    searchRequest.source(searchSourceBuilder);

    try {
      SearchResponse response = client.search(searchRequest);
      SearchHit[] searchHits = response.getHits().getHits();
      String scrollId = response.getScrollId();

      while (searchHits != null && searchHits.length > 0) {
        Stream.of(searchHits).forEach(hit -> this.addBar(series, hit));
        SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
        scrollRequest.scroll(scroll);
        response = client.scroll(scrollRequest, RequestOptions.DEFAULT);
        scrollId = response.getScrollId();
        searchHits = response.getHits().getHits();
      }
      
      ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
      clearScrollRequest.addScrollId(scrollId);
      ClearScrollResponse clearScrollResponse = client.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
      boolean succeeded = clearScrollResponse.isSucceeded();
      if (!succeeded) {
        System.err.println("Succeded was false");
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return series;
  }

  public List<String> fectchAvailableSymobls() {
    RestHighLevelClient client = ClientFactory.getInstance().getClient();

    try {
      SearchSourceBuilder source = new SearchSourceBuilder()
        .query(QueryBuilders.matchAllQuery())
        .aggregation(
          AggregationBuilders
              .terms("symbols")
              .field("symbol")
              .order(BucketOrder.key(true))
              .size(9999));
      SearchRequest request = new SearchRequest("bars").source(source);
      SearchResponse response = client.search(request);
      ParsedStringTerms terms = response.getAggregations().get("symbols");
      return terms.getBuckets().stream().map(bucket -> bucket.getKeyAsString()).collect(Collectors.toList());
    } catch(IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void addBar(TimeSeries series, SearchHit hit) {
    Map<String, Object> map = hit.getSourceAsMap();
    ZonedDateTime date = LocalDateTime.parse(map.get("date").toString(), formatter).atZone(ZoneId.of("US/Eastern"));
    Double open = Double.parseDouble(map.get("open").toString());
    Double close = Double.parseDouble(map.get("close").toString());
    Double high = Double.parseDouble(map.get("high").toString());
    Double low = Double.parseDouble(map.get("low").toString());
    Double volume = Double.parseDouble(map.get("volume").toString());
    series.addBar(date, open, high, low, close, volume);
  }

}